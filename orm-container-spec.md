# Owned data containers: specification

This document consolidates the decisions in `orm-container.md`. The original
file remains the raw discussion record. This specification separates agreed
behavior, implementation requirements, and deferred work; it does not finalize
backend interfaces or introduce implementation code.

## 1. Scope and public types

Introduce two data-node containers:

| Public class | Data entry point | Contents |
| --- | --- | --- |
| `orm.DataList` | `core.data_list` | Ordered data nodes |
| `orm.DataDict` | `core.data_dict` | String keys mapped to data nodes |

Existing `orm.List` and `orm.Dict` retain their plain-value behavior unchanged.

Elements are independent, exclusively owned copies of `orm.Data` instances.
Any data subclass is eligible, including nested containers, subject to the
homogeneity requirement below. Process nodes and other non-data nodes are
rejected. Elements use ordinary node entry points; no per-element-type
container registrations or plugin opt-in mechanism are introduced.

Only the new container types may own nodes. Any shared container base remains
internal initially; its interface follows the storage design.

The following are separate proposals, not part of this feature:

- Attribute schemas/specifications and model versioning.
- Domain-specific element models and container hierarchies.
- Compact structure-of-arrays representations and numerical array layout.

## 2. Relational storage and ownership

### Membership

Store containment in a dedicated membership table, not node attributes or the
existing provenance link table. Each membership row references its immediate
owner and child through foreign keys and persists position and, for dictionary
entries, a string key.

Membership rows are containment edges, not computational provenance. Do not
duplicate them as provenance links or repurpose CREATE, RETURN, or INPUT links
for containment. Membership-link labels and UUID-based membership attributes
are not needed.

### Explicit owner reference

Add an indexed, nullable owner foreign key to the base node table:

- Null: standalone node.
- Non-null: exclusively owned node.

No separate ownership-mode flag is needed. Ownership checks and deletion
traversal use this reference rather than inferring ownership from provenance
links.

### Invariants

- Each owned child has exactly one membership row from its immediate owner.
- Every membership row agrees with the child's owner reference.
- Ownership is exclusive and acyclic.
- Owners must be eligible containers; children must be eligible data nodes.
- Owner references and membership rows are created atomically.
- Stored ownership and membership cannot be changed in place.

Enforce these invariants through the common backend contract, including
individual writes, bulk operations, low-level provenance links, deletion, and
archive import. ORM convenience methods are not the enforcement boundary.

Use database foreign keys and uniqueness constraints as structural safeguards,
with transactional backend validation for remaining rules. Database triggers
are not the initial strategy. Transactions alone do not provide complete
concurrency safety; the concrete constraints, locking/validation strategy, and
write ordering remain implementation design work.

Maintain consistent PostgreSQL and SQLite behavior.

## 3. Element-type metadata and homogeneity

List elements and dictionary values must have the same exact concrete node
type. Sharing a base class is insufficient.

Infer the runtime type from the first element and persist its existing
`node_type` identifier in dedicated relational container metadata, independent
of membership rows. Do not store it in node attributes or introduce a separate
Python class-path field or compatibility registry.

- A container that has never held an element has an unset type, stored as NULL.
- Such an empty container may be stored.
- NULL means unset, not the Python type `NoneType`.
- Once established, the type remains locked, including after removing every
  element. Later insertions must match it.
- Backend checks compare stored identifiers without loading element plugins.
- Verify that existing identifiers distinguish the intended concrete types.

For nested containers, compare only their concrete container node type, not
inner element-type metadata. An outer list may contain nested lists with
different inner element types; each nested container enforces its own rule.

Generic annotations should preserve element types for static checking but do
not populate runtime metadata or enforce exact runtime-class equality.
Finalize the ORM hierarchy and generic interfaces against storage operations.
The exact relational metadata schema remains implementation design work.

## 4. Copying and lifetime

Insertion always creates an independent copy. It does not adopt, delete, or
change ownership of the source. Repeated insertion of the same source creates
distinct owned copies. Inserting an explicitly created copy copies it again.

Ownership cannot be transferred in place. Copying an owned child creates a
standalone node with a new identity and no inherited ownership or provenance
links; the original stays in the container.

Copying a container recursively copies its complete owned subtree. The copied
root is standalone, and copied descendants belong to the new subtree.

Follow existing `Data.clone()` metadata behavior: preserve attributes,
repository content, extras, labels, descriptions, and user/computer
associations. Mutable metadata must be independent between copies. Container
copying adds recursive ownership handling, not a separate metadata policy.

Do not record a persistent source-copy relationship: no source UUID metadata,
dedicated copy relation, or invented process execution or provenance link.

Copies must remain usable after deletion of their source or another copy.
Logical independence does not require physically duplicating content-addressed
repository objects.

## 5. Mutation, storage, and freezing

Before storage, permit membership insertion, replacement, removal, and list
reordering. Owned children remain mutable but cannot be stored independently.

Removing or replacing an element before storage clears the removed child's
in-memory ownership and excludes it from the former container's active subtree.
The removed child becomes standalone, unstored, and editable; retained references
remain usable and may store it independently. For a removed nested container,
only its root is detached; its descendants remain owned by it. Removal does not
store or invalidate the removed node, and its lifetime follows ordinary Python
references and garbage collection. This pre-storage detachment does not permit
reparenting in place: inserting the removed node elsewhere still copies it.
Stored membership remains immutable.

Storing the ownership root freezes and persists its entire owned subtree,
owner references, membership rows, and element-type metadata atomically.
Children cannot be frozen while their parent remains mutable.

- Success: the entire subtree is stored; membership and child content become
  immutable together, subject to ordinary node mutability exceptions such as
  extras.
- Failure: no partially stored ownership graph remains, and the entire subtree
  remains unstored and unfrozen.

Recovery also covers rollback of an enclosing transaction or savepoint after
container storage has returned successfully. Restore the same retained root
and child objects to an unstored, editable state, preserving content,
membership, ordering, and established element-type metadata. Storage must be
retryable; replacing the root with a fresh object or requiring callers to
discard retained children does not satisfy this guarantee. Container recovery
must participate in the enclosing transaction lifecycle without requiring a
global change to existing non-container node rollback behavior.

Follow the existing repository-first storage approach for the complete subtree:

1. Validate the complete subtree.
2. Prepare all repository content before writing the subtree's database records.
3. Write nodes, owner references, membership rows, and element-type metadata in
   one database transaction, joining an enclosing transaction when present.

Database rollback does not reverse repository writes. Unreferenced permanent
repository objects may remain pending safe cleanup through normal storage
maintenance; immediate physical deletion is not required. Clean up temporary
resources no longer needed by the recovered objects, and never leave a persisted
partial ownership graph.

Rollback recovery must preserve readable, editable, retryable content for the
same retained unstored objects, independently of subsequent orphan cleanup.
Coordinate in-memory state and repository recovery using existing cloning and
repository mechanisms; the concrete recovery mechanism remains implementation
work. Apply the same resource-lifetime principles to failed or superseded copy
operations.

## 6. Ordering, keys, and Python behavior

Both membership kinds persist positions; database row order is not meaningful.
ORM iteration follows persisted position order after reloading.

Dictionary keys are strings only. Reject non-string keys rather than coercing
them. Follow Python dictionary insertion-order behavior:

- Replacing a value preserves the key's position.
- Removing and reinserting a key moves it to the end.

Provide sequence/mapping behavior through length, indexing or key lookup, and
iteration. Do not base the API on `get_list()` or `get_dict()` or change those
methods on existing containers. The first iteration exposes only intentionally
minimal ORM scaffolding; the complete API follows later.

Equality follows base `Node` identity behavior, comparing UUIDs. Independent
copies are unequal even when content matches. Python hashing remains UUID-based
and is separate from the content-based caching hash.

Slicing is not supported initially.

## 7. Process boundaries

### Inputs

When an owned child is supplied as a process input, create an independent
standalone copy. The process must execute with the same copy recorded as its
provenance input. Copy an owned nested container's complete subtree.

A standalone container is linked directly as an input; its descendants remain
owned by it. All copies remain subject to normal provenance validation.

### Outputs

- **Calculations:** exposing an owned child creates an independent standalone
  copy and attaches CREATE to that copy. Copy nested containers recursively.
  The exposed output is the same object recorded in provenance, and ordinary
  CREATE validation applies.
- **Workflows:** may return a whole standalone container under ordinary RETURN
  validation. Reject separately returning owned children, including owned
  nested containers. Do not automatically copy workflow outputs.

Reject direct low-level provenance links to owned children. Automatic copying
belongs at the process input/calculation-output boundary, whose concrete
integration points remain implementation work.

### Ports

Initially use existing class-only `valid_type` validation for `DataList` and
`DataDict`. Internal homogeneity is still enforced. Parameterized generic types
do not automatically implement runtime port validation.

## 8. Deletion

Deleting a container includes its complete owned subtree, subject to ordinary
provenance deletion safeguards. Owned children cannot be deleted independently.
Original sources are not deleted merely because they were copied into a
container.

Integrate ownership closure into existing connected-node deletion planning:

1. Expand a child-targeted request to its ownership root and complete subtree.
2. Apply ordinary provenance deletion rules.
3. Present the final expanded set for confirmation or explicit approval.
4. At execution, validate the approved set without enlarging it.

CLI `--force` explicitly approves expansion without prompting. Programmatic
callers must explicitly approve ownership expansion; `dry_run=False` alone is
not approval.

Without approval, reject execution. If the approved set becomes invalid, fail
rather than silently adding nodes. Database cascades alone are not sufficient.

## 9. Groups

A group contains a complete ownership unit or none of it. Store explicit group
membership rows for the root and every descendant so ordinary group iteration
and queries include all nodes without special traversal.

Adding or removing a container's membership updates the whole unit atomically.

- Python API: reject child-targeted add/remove requests; require the root.
- CLI: show the expansion and obtain confirmation before applying it.
- Without confirmation, reject CLI expansion.

Enforce complete group membership through ORM, backend, and import paths.
Deleting a group normally does not delete nodes. Operations that also delete
nodes use ownership-aware deletion planning.

Other external reference mechanisms remain an audit item; identify actual
mechanisms before deciding whether additional policy is necessary.

## 10. Caching hashes

Container caching hashes are content-based and incorporate child content
recursively, excluding owner IDs and child UUIDs. Equivalent independent copies
have equal hashes.

- Lists include element order.
- Dictionaries include keys and values, but ignore insertion order.
- Persisted element-type metadata contributes, including its NULL state.
  Differently typed empty containers therefore have different hashes.

Cache reuse must preserve independent ownership subtrees and never introduce
shared ownership of child nodes.

## 11. Archives and missing plugins

### Archive closure

Export the complete ownership unit. Selecting an owned child includes its root
and all descendants, including siblings. Apply the same closure to group-based
selection.

### Import

Reuse existing UUID deduplication:

- Insert new UUIDs.
- Reuse existing nodes only when ownership, membership, positions, keys, and
  established element types agree.
- Treat disagreement under one UUID as an integrity error; reject and roll back
  import rather than skip inconsistent nodes or reconcile ownership.

Do not change ownership, detach a node, or regenerate UUIDs to resolve a
conflict.

Existing standalone nodes remain standalone through migration. Existing plain
`List` and `Dict` content requires no conversion into owned nodes. Add backend
and archive schema support without inventing historical executions or CREATE
links.

### Missing element plugins

Structural inspection, ownership traversal, deletion under normal safeguards,
and archive export/import must work through storage records without element
plugins. This includes positions, keys, and stored type identifiers.

Typed element access and plugin-specific operations require the real plugin and
report its absence. Container element loading must detect missing plugins rather
than silently accept the existing `load_node_class()` fallback to base `Data`.
Keep ordinary node-loading fallback behavior unchanged outside container typed
access. A genuine base `Data` element remains valid and must not be mistaken for
a missing-plugin fallback. Structural operations continue using storage records;
do not introduce substitute element classes.

### Content exports

An available individual-node exporter, such as a YAML exporter, may export the
selected node's represented content without its parent or siblings. Export does
not detach or modify the node. This does not introduce a universal container
serialization format.

## 12. Implementation staging

The first iteration focuses on correct relational storage:

- Schema, owner reference, membership, and element-type metadata.
- In-memory representation before database IDs exist.
- Backend operations, validation, concurrency, and atomic subtree storage.
- Backend-level invariant and failure-path tests.
- Integration requirements for deletion, groups, archives, and cache ownership.

Storage correctness must not depend on ORM convenience methods. Introduce
intentionally minimal ORM and QueryBuilder scaffolding in separate commits on
top of storage work; do not treat that scaffolding as the complete feature.

This staging requirement concerns implementation coverage in the source code,
not a node's runtime state or eligibility for storage. At every implementation
stage, each exposed affected operation must preserve the specified invariants
or explicitly reject unsupported container/owned-child operations. This applies
before and after storage, including deletion, groups, archives, cache reuse,
and automatic grouping during `Node.store()`. Existing behavior must not silently
bypass ownership enforcement. Safe rejection permits incremental implementation
but does not make a deferred integration feature-complete.

Initial QueryBuilder support:

- Container-to-immediate-child and child-to-owner joins.
- Membership position/key filters and persisted element-type filters.
- Containers, children, or both as results under ordinary join semantics.
- Filters on one child join match that child; separate joins may match different
  children.

Verify subclass discovery against existing stored type-string query behavior,
not Python inheritance alone.

Concrete tables, constraints, backend signatures, transaction recovery, and
repository cleanup remain implementation design work. Preparation is described
separately in `orm-container-storage-preparation.md` and
`storage-savepoint-failure-preparation.md`.

## 13. Deferred implementation

Deferral does not promise a final API or timeline. Later work must preserve the
agreed ownership and storage invariants.

- **Full ORM API and typing:** comprehensive sequence/mapping operations,
  finalized internal container abstraction, and generic annotations beyond the
  initial scaffolding. Backend validation is not deferred.
- **Slicing:** intended to create an independent copied ORM container, not a
  view or shared ownership.
- **Advanced queries:** recursive membership traversal; specialized any/all,
  count, and empty-container predicates; automatic container-specific
  deduplication. Define recursive matching and result semantics first.
- **Element-type-aware ports:** explicit runtime integration for parameterized
  port types, including empty-container and subclass-matching rules.
- **Container-specific serialization:** no new generic YAML/JSON schema or
  serialization API initially. Archive requirements are not deferred by this.
- **Storage-representation-based homogeneity:** possible future abstraction;
  exact existing node-type identifiers remain the initial rule.

Out-of-scope proposals in section 1 are not promised follow-ups. Rejected
features, such as persistent source-copy tracking, are not deferred work.

## 14. Verification and performance

Test both PostgreSQL and SQLite, including archive round-trips. Require storage
invariant tests in the first iteration; test deferred APIs when implemented.

Cover:

- Copy-on-insertion, repeated insertion, metadata independence, and unchanged
  ownership/lifetime of sources.
- Recursive cloning and repository independence.
- Owner/membership agreement through low-level and convenience paths.
- Eligibility, homogeneity, type locking, and typed/untyped empty containers.
- Cycle rejection and concurrent invariant violations.
- Pre-storage mutation, removal, ordering, and dictionary keys.
- Independent-child storage rejection, atomic freezing, rollback, and retained
  object state after failure.
- Process input copies, calculation output copies, workflow output rejection,
  and direct low-level provenance-link rejection.
- Ownership-expanded deletion approval and execution-time revalidation.
- Complete-unit group membership and Python/CLI add/remove behavior.
- Archive identity conflicts, migration, and missing-plugin operations.
- Initial membership queries and subclass discovery.
- Content hashing and independent ownership during cache reuse.
- Existing plain-value `List` and `Dict` behavior remaining unchanged.

Measure storage, loading, copying, traversal, hashing, export, and deletion at
realistic collection sizes. Each element adds a node, owner reference, and
membership row. Consider bulk operations and avoid repeated per-child queries
without weakening validation.

Consumers associating positional arrays with elements must preserve alignment
through filtering and copying. Stable element identification and ordering must
be explicit; numerical array layout itself remains outside this contract.
