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
class TaskView:
    """The writable surface one task sees inside an execution scope.

    A view exposes only its own staged entries; other views are unreachable
    through it. The boundary merges views in creation order.
    """

    batch: 'WriteBatch | None' = None
    staged: StagedFields = field(default_factory=dict)


@dataclass
class WriteBatch:
    """Pending fields belonging to one stored node, not a shared SQL session."""

    model: object
    originals: StagedFields = field(default_factory=dict)
    views: list[TaskView] = field(default_factory=list)
    active: bool = True


class DeferredWrites:
    """Keep node metadata separate from SQLAlchemy until an explicit boundary."""

    # Extras are also used by plugins for live cross-task coordination.
    FIELDS: t.ClassVar[frozenset[FieldName]] = frozenset({'attributes', 'label', 'description', 'repository_metadata'})

    def __init__(self, backend: SqlStorage) -> None:
        self.backend = backend
        self._batches: dict[int, WriteBatch] = {}
        self._current: ContextVar[TaskView | None] = ContextVar('storage_write_view', default=None)
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
        owner = TaskView(batch)
        batch.views.append(owner)
        token = self._current.set(owner)
        applying_token = self._applying.set(False)
        try:
            yield
            if any(value != batch.originals[name] for view in batch.views for name, value in view.staged.items()):
                msg = 'Deferred writes remain without an explicit persistence boundary.'
                raise exceptions.InvalidOperation(msg)
        finally:
            batch.active = False
            batch.views.clear()
            batch.originals.clear()
            self._batches.pop(id(model))
            self._applying.reset(applying_token)
            self._current.reset(token)

    @contextmanager
    def task_scope(self) -> Iterator[TaskView | None]:
        """Hand the current task its own view onto the active batch.

        Tasks created inside an execution scope inherit the owner's view
        through the copied context; entering this scope swaps in a fresh one
        whose staged entries are invisible to siblings and unreachable from
        them. Same-field writes across views fail loudly at the boundary
        instead of silently overwriting each other. Outside a batch this
        yields ``None`` and node writes commit immediately. The yielded view
        may be held and passed explicitly; routing still follows the current
        view so existing call sites need no new parameters.
        """
        view = self._current.get()
        batch = view.batch if view is not None else None
        if not self.backend._batch_writes or batch is None or not batch.active or self._applying.get():
            yield None
            return
        fresh = TaskView(batch)
        batch.views.append(fresh)
        token = self._current.set(fresh)
        try:
            yield fresh
        finally:
            self._current.reset(token)

    def owns(self, model: object, name: str) -> bool:
        view = self._current.get()
        batch = view.batch if view is not None else None
        return (
            self.backend._batch_writes
            and not self._applying.get()
            and batch is not None
            and batch.active
            and batch.model is model
            and name in self.FIELDS
        )

    def get(self, model: object, name: FieldName) -> StagedValue:
        """Read the current view's staged value, else the committed value, without dirtying the session."""
        view = self._current.get()
        assert view is not None and view.batch is not None and self.owns(model, name)
        batch = view.batch
        if name not in batch.originals:
            session = self.backend.get_session()
            with session.no_autoflush:
                session.refresh(model, attribute_names=[name])
                batch.originals[name] = deepcopy(getattr(model, name))
        value = view.staged[name] if name in view.staged else batch.originals[name]
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
        view = self._current.get()
        assert view is not None and self.owns(model, name)
        self.get(model, name)
        view.staged[name] = deepcopy(value)
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
        """Return the current view's batch if it is still staging."""
        view = self._current.get()
        batch = view.batch if view is not None else None
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
                    for name, value in self._merge_views(batch).items():
                        merged = self._merge(name, batch.originals[name], value, getattr(batch.model, name))
                        setattr(batch.model, name, merged)
                yield
        finally:
            if batch is not None:
                # Clear per-view staged entries too: holders keep referencing
                # their view after the boundary commits or rolls back.
                for view in batch.views:
                    view.staged.clear()
                batch.views.clear()
                batch.originals.clear()
            self._applying.reset(token)

    @staticmethod
    def _merge_views(batch: WriteBatch) -> StagedFields:
        """Combine task views; identical staged values merge, conflicts raise."""
        combined: StagedFields = {}
        for view in batch.views:
            for name, value in view.staged.items():
                if value == batch.originals[name]:
                    continue
                if name in combined:
                    if value != combined[name]:
                        msg = f'Concurrent update of deferred field `{name}` by tasks in one execution.'
                        raise exceptions.InvalidOperation(msg)
                    continue
                combined[name] = deepcopy(value)
        return combined
