###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
###########################################################################
"""Storage-owned metadata staging without an open write transaction."""

import typing as t
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, field

from sqlalchemy import event
from sqlalchemy.orm import ORMExecuteState, Session, SessionTransaction

from aiida.common import exceptions

FieldName: t.TypeAlias = t.Literal['attributes', 'label', 'description', 'repository_metadata']
"""Model fields that stage in an execution scope instead of committing immediately."""

JsonValue: t.TypeAlias = 'bool | int | float | str | list[JsonValue] | dict[str, JsonValue] | None'
JsonObject: t.TypeAlias = dict[str, JsonValue]

StagedValue: t.TypeAlias = JsonObject | str
"""A replacement for a supported node metadata column."""

StagedFields: t.TypeAlias = dict[FieldName, StagedValue]
"""Staged field values keyed by model field name."""


class SqlStorage(t.Protocol):
    """SQL operations required at a persistence boundary."""

    _batch_writes: bool

    def get_session(self) -> Session: ...

    def transaction(self) -> AbstractContextManager[Session]: ...


@dataclass
class WriteBatch:
    """Pending fields belonging to one stored node, not a shared SQL session."""

    model: object
    originals: StagedFields = field(default_factory=dict)
    staged: StagedFields = field(default_factory=dict)
    active: bool = True


class DeferredWrites:
    """Keep node metadata separate from SQLAlchemy until an explicit boundary."""

    # Extras are also used by plugins for live cross-task coordination.
    FIELDS: t.ClassVar[frozenset[FieldName]] = frozenset({'attributes', 'label', 'description', 'repository_metadata'})

    def __init__(self, backend: SqlStorage) -> None:
        self.backend = backend
        self._batches: dict[int, WriteBatch] = {}
        self._current: ContextVar[WriteBatch | None] = ContextVar('storage_write_batch', default=None)
        self._applying: ContextVar[bool] = ContextVar('storage_applying_batch', default=False)

    @contextmanager
    def scope(self, model: object) -> Iterator[None]:
        """Associate writes with an execution; never inherit another node's batch."""
        if not self.backend._batch_writes:
            yield
            return
        session = self.backend.get_session()
        if session.in_nested_transaction():
            msg = 'Deferred writes cannot run inside an explicit caller transaction.'
            raise exceptions.InvalidOperation(msg)
        self._watch_session(session)
        if id(model) in self._batches:
            msg = 'A deferred execution scope already owns this node.'
            raise exceptions.InvalidOperation(msg)
        batch = WriteBatch(model)
        self._batches[id(model)] = batch
        token = self._current.set(batch)
        applying_token = self._applying.set(False)
        try:
            yield
            if any(value != batch.originals[name] for name, value in batch.staged.items()):
                msg = 'Deferred writes remain without an explicit persistence boundary.'
                raise exceptions.InvalidOperation(msg)
        finally:
            batch.active = False
            batch.staged.clear()
            batch.originals.clear()
            self._batches.pop(id(model))
            self._applying.reset(applying_token)
            self._current.reset(token)

    def owns(self, model: object, name: str) -> bool:
        batch = self._current.get()
        return (
            self.backend._batch_writes
            and not self._applying.get()
            and batch is not None
            and batch.active
            and batch.model is model
            and name in self.FIELDS
        )

    def get(self, model: object, name: FieldName) -> StagedValue:
        """Read the staged value, else the committed value, without dirtying the session."""
        batch = self._current.get()
        assert batch is not None and self.owns(model, name)
        if name not in batch.originals:
            session = self.backend.get_session()
            with session.no_autoflush:
                session.refresh(model, attribute_names=[name])
                batch.originals[name] = deepcopy(getattr(model, name))
        value = batch.staged[name] if name in batch.staged else batch.originals[name]
        self.release_read_transaction()
        return deepcopy(value)

    @t.overload
    def set(self, model: object, name: t.Literal['label', 'description'], value: str) -> None: ...

    @t.overload
    def set(self, model: object, name: t.Literal['attributes', 'repository_metadata'], value: JsonObject) -> None: ...

    def set(self, model: object, name: FieldName, value: StagedValue) -> None:
        """Snapshot a supported field replacement already handled by the node setters."""
        if self.backend.get_session().in_nested_transaction():
            msg = 'Deferred metadata writes cannot overlap an explicit caller transaction.'
            raise exceptions.InvalidOperation(msg)
        batch = self._current.get()
        assert batch is not None and self.owns(model, name)
        self.get(model, name)
        batch.staged[name] = deepcopy(value)
        self.release_read_transaction()

    @staticmethod
    def _watch_session(session: Session) -> None:
        """Distinguish clean read transactions from already-flushed SQL writes."""
        if session.info.get('aiida_tracks_deferred_writes'):
            return
        session.info['aiida_tracks_deferred_writes'] = True

        def flushed(session: Session, context: object) -> None:
            session.info['aiida_transaction_has_writes'] = True

        def executed(state: ORMExecuteState) -> None:
            if state.is_insert or state.is_update or state.is_delete:
                state.session.info['aiida_transaction_has_writes'] = True

        def ended(session: Session, transaction: SessionTransaction) -> None:
            if transaction.parent is None:
                session.info.pop('aiida_transaction_has_writes', None)

        event.listen(session, 'after_flush', flushed)
        event.listen(session, 'do_orm_execute', executed)
        event.listen(session, 'after_transaction_end', ended)

    def _active_batch(self) -> WriteBatch | None:
        """Return the current batch if it is still staging."""
        batch = self._current.get()
        if batch is None or not batch.active:
            return None
        return batch

    def defer_commit(self) -> bool:
        """Delay metadata commits, but never swallow unrelated dirty session writes."""
        session = self.backend.get_session()
        return (
            self.backend._batch_writes
            and self._active_batch() is not None
            and not self._applying.get()
            and not session.new
            and not session.dirty
            and not session.deleted
            and not session.info.get('aiida_transaction_has_writes')
        )

    def release_read_transaction(self) -> None:
        """Do not retain even a read transaction while an execution awaits I/O."""
        session = self.backend.get_session()
        if (
            self._active_batch() is not None
            and not self._applying.get()
            and not session.in_nested_transaction()
            and not session.new
            and not session.dirty
            and not session.deleted
            and not session.info.get('aiida_transaction_has_writes')
        ):
            session.rollback()

    @staticmethod
    def _merge(name: str, original: object, value: object, current: object) -> object:
        """Apply only changed keys, rejecting overlapping external updates."""
        if isinstance(original, dict) and isinstance(value, dict) and isinstance(current, dict):
            result = deepcopy(current)
            missing = object()
            for key in original.keys() | value.keys():
                before = original.get(key, missing)
                after = value.get(key, missing)
                if before == after:
                    continue
                actual = current.get(key, missing)
                if actual not in (before, after):
                    msg = f'Concurrent update of deferred field `{name}.{key}`.'
                    raise exceptions.InvalidOperation(msg)
                if after is missing:
                    result.pop(key, None)
                else:
                    result[key] = deepcopy(after)
            return result
        if current not in (original, value):
            msg = f'Concurrent update of deferred field `{name}`.'
            raise exceptions.InvalidOperation(msg)
        return deepcopy(value)

    @contextmanager
    def boundary(self, model: object | None = None) -> Iterator[None]:
        """Merge field-level changes into authoritative data and commit once."""
        batch = self._active_batch()
        if model is not None:
            batch = self._batches.get(id(model))
        if batch is not None and not batch.active:
            batch = None
        if self._applying.get():
            with self.backend.transaction():
                yield
            return
        token = self._applying.set(True)
        try:
            with self.backend.transaction():
                session = self.backend.get_session()
                target = batch.model if batch is not None else model
                if target is not None:
                    session.refresh(target, with_for_update=True)
                if batch is not None:
                    for name, value in batch.staged.items():
                        if value == batch.originals[name]:
                            continue
                        merged = self._merge(name, batch.originals[name], value, getattr(batch.model, name))
                        setattr(batch.model, name, merged)
                yield
        finally:
            if batch is not None:
                batch.staged.clear()
                batch.originals.clear()
            self._applying.reset(token)
