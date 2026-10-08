# Branch goal: LORIS Resource Model and data-management tools

This branch explores an extensible logical resource model layered above the existing SQLAlchemy
ORM. It should describe LORIS domain objects that may span several database tables and local
files, expose typed queryable properties, and connect objects through explicit relationships.
The initial consumer is a robust imaging-data inspector/deleter, but the model should also be
usable for inventory, integrity checking, backup, and other data-management operations.

Everything currently implemented on this branch is highly experimental. No public API or current
class structure should be treated as stable: change or replace the architecture whenever a simpler
design can be tested through a useful vertical slice.

There are no current consumers and no compatibility guarantees. Do not preserve old APIs, command
names, entry points, import paths, wrappers, aliases, or deprecated behavior when changing the
design unless the user explicitly requests compatibility for a specific interface. Prefer removing
superseded interfaces as part of the same change.

## Design direction

- Keep SQLAlchemy as the database-row model; do not build a replacement ORM.
- Treat object references as session-independent, but resolved logical and database-row objects as
  short-lived wrappers around ORM instances belonging to the current SQLAlchemy session. Derive
  their properties and resource bindings from those instances instead of copying ORM state into a
  parallel snapshot.
- Keep the SQLAlchemy session open while consuming resolved objects. Providers should eager-load
  the relationships that define a logical object; do not add a separate resource context until a
  concrete use case requires multiple sessions or non-database services.
- Define generic, inspectable properties as class-level `ObjectProperty` metadata. Property
  definitions own value extraction and optional query behavior; do not duplicate ORM attributes
  with convenience getters on logical-object instances.
- Address properties through stable qualified `PropertyRef` names such as `project.name` and pass
  already-typed values through `PropertyCriterion`. Generic consumers should resolve and
  introspect properties through the schema rather than importing provider-specific constants.
- Use kebab-case for public semantic-schema identifiers (for example, `dicom-archive`,
  `session.visit-label`, and relationship names). Python and physical database identifiers may
  remain snake_case.
- Give each property an explicit `PropertyType` for runtime type metadata and text parsing. Keep
  its current single query behavior in `PropertyQuery`, whose operand type may differ from the
  displayed property value type (for example, an integer property queried by an integer set).
- Use generic `ObjectSelection` values containing stable object references and property predicates
  at provider boundaries. Keep user-interface request models and cross-object traversal separate
  from these local, single-object-kind selections.
- Register providers and query-capable relationship bindings with the schema. Property selection
  may automatically traverse a unique transitive chain of `belongs-to` relationships; missing or
  ambiguous paths must fail rather than choosing an arbitrary interpretation. Other relationship
  semantics require explicit traversal and are not currently part of property lookup.
- Treat each `RelationshipBinding` as the authoritative operational definition of its relationship:
  it owns both forward target discovery for graph edges and reverse source selection for queries.
  Providers only locate logical objects and must not duplicate relationship construction.
- Keep relationships distinct from scalar properties. Relationships have source-scoped semantic
  member names such as `session.project`; their bindings use private ORM-aware selection
  constraints rather than exposing physical foreign keys as semantic properties. Property paths
  may compose them, for example `session.project.name` or
  `dicom-archive.session.project.id`.
- Distinguish registered `ObjectKind` definitions from concrete resolved objects such as
  `SessionObject` and `DicomArchiveObject`.
- Keep modality-specific discovery in typed providers. The common layer owns graph composition,
  validation, planning, and eventually execution.
- Represent logical objects, concrete database rows, and storage-root-relative paths as typed
  objects in one heterogeneous resource graph. Keep logical objects as the semantic query boundary;
  physical objects do not automatically become semantic-schema kinds or properties.
- Use generic `DatabaseRowObject[ModelT]` wrappers around persistent ORM instances and derive
  session-independent `DatabaseRowRef` identities from SQLAlchemy `Table` metadata and mapper
  identity. Do not duplicate physical table or primary-key names as unchecked strings in providers.
- Represent both local files and directories with `LocalPathObject`, using `expected_type` to record
  what the provider expects without claiming that current filesystem state has been verified.
  Symlink discovery and handling are deferred until filesystem revalidation is designed.
- Connect logical objects to physical objects through explicit resource bindings. The initial
  binding semantics are deliberately limited to `owns` and `references`; do not infer ownership
  solely from foreign keys.
- Make ownership, sharing, provenance, and deletion behavior explicit rather than inferring them
  solely from foreign keys.
- Allow external LORIS modules to register namespaced kinds, relationships, resolvers, and policies.
- Keep inspection read-only. Future deletion must default to planning/dry-run, back up resources,
  revalidate before execution, and use one database transaction.
- Do not add database tables or columns for this feature.

## Current proof of concept

The experimental code is in the `python/loris_data_manager/` workspace package. It currently
models:

- sessions;
- projects and sites as shared session context;
- DICOM archives, including `tarchive`, series, file, and MRI-upload rows;
- local DICOM archive paths;
- DICOM-archive-to-session relationships;
- session-to-project and session-to-site relationships;
- typed logical and physical resource objects, explicit resource bindings, generic selections, and
  composable partial resource graphs.

The package's `loris-data-manager inspect` command is a read-only CLI with text and JSON output.
Repeatable `--select` expressions project whole logical objects or individual properties, while
repeatable qualified `--where` filters constrain the query and are combined using AND. Object IDs
are ordinary typed, queryable semantic properties. Explicit relationship paths such as
`--where 'dicom-archive.session.project.name=Brainstorm'` are supported. Selection may traverse
`belongs-to` relationships in either direction when there is exactly one path. Intermediate
traversal objects do not become output automatically.

Anchor inference is intentionally provisional: all `--select` expressions must currently share
one root, which becomes the query anchor. Filters do not determine the anchor, so an archive
projection can be independently constrained by `project.alias` and `site.alias`. Revisit this rule
before treating the CLI query language as stable. `ObjectRef` selection remains an internal
mechanism for relationship resolution and graph expansion.
Project and site names and aliases can select those objects directly or scope sessions. Selection
by project, site, or session properties is propagated generically through registered `belongs-to`
bindings, including transitive DICOM-to-session-to-project/site paths. Query intermediaries do not
leak into results unless selected or expanded as related context.

Relationship expansion is intentionally outgoing only. Expanding a session resolves its shared
project and site, while expanding a project or site does not implicitly enumerate every session.
The current `--expand-related` behavior is a convenience of the inspection CLI for displaying
related context; it is not required for property filtering, relationship-path traversal, or graph
composition. Generalizing expansion across registered object kinds is therefore deferred until a
consumer needs complete contextual graphs or configurable traversal.

Important schema finding: current DICOM import code sets `tarchive.SessionID` to `NULL`; session
association commonly comes through `mri_upload(TarchiveID, SessionID)`. Providers must account for
both paths.

## Open design work

Prioritize proving the model through usable tools rather than growing an abstract framework.
Likely next steps are:

1. Improve the inspection/query API and provider registration model.
2. Refine resource-binding semantics beyond the initial `owns` and `references` only when concrete
   use cases require distinctions such as generated or unmanaged resources.
3. Define relationship traversal and operation policies separately from descriptive relationships.
4. Extend property predicates beyond their current single filter operation when concrete querying
   needs establish the operator model.
5. Address graph completeness, stable identities, batching for large selections, plugin versioning,
   database-row snapshots, and legacy absolute paths.
6. Only then prototype deletion planning, SQL/filesystem backup, revalidation, and execution.

A physiological-recording vertical slice is deliberately out of scope for this branch. The current
SQL representation around electrodes, coordinate systems, and points is too convoluted to provide
a clean test of the logical resource model. Simplify that database model separately before using
physiological recordings to validate shared resources, parent recordings, sidecars, events, chunks,
and directories.

Do not assume that a foreign key or ORM relationship implies lifecycle ownership. Unknown or
ambiguous relationships should fail closed for destructive operations.

## Validation

Run from the repository root:

```bash
uv run ruff check python/loris_data_manager
uv run pyright python/loris_data_manager
uv run pytest
```

The existing untracked `uv.lock` is unrelated user/workspace state; preserve it.
