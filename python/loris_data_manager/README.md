# LORIS data manager

Experimental logical resource model and data-management tools for LORIS.

The package currently provides a read-only resource inspector, verified resource backups, and a
backup-first DICOM archive deletion proof of concept. Its API is unstable while the resource model
and operation semantics are developed through concrete vertical slices.

## DICOM archive deletion proof of concept

The first deletion vertical slice requires a newly created, verified backup. Backup-free
deletion may become an explicit policy later, but backup must not be inseparable from the internal
deletion architecture. A dry-run only plans the operation and does not require a backup.

Backup and deletion must use the same materialized physical plan, database connection,
transaction, and locks. The shared backup transaction therefore exposes its verified, locked
capture to the deletion action rather than calling standalone `create_backup()` and subsequently
opening a new transaction, which would introduce a time-of-check/time-of-use gap.

The intended execution sequence is:

1. Begin one explicit repeatable-read database transaction.
2. Resolve the semantic selection, lock the anchor rows, and freeze their exact identities.
3. Build the ownership closure and incoming-reference plan from those identities. Lock all rows
   that will be deleted or updated, and fail on unapproved references.
4. Copy all owned filesystem entries and serialize all deleted rows and reference-restoration
   patches into a durable backup. Regular files are copied by value; symbolic links are preserved
   shallowly without reading their targets. An expected path that is already absent is recorded
   as missing rather than given a payload. Finalize and verify that artifact before any destructive
   change.
5. Apply approved nullable-reference updates, then delete owned database rows child-to-parent,
   checking exact affected-row counts. Do not commit yet.
6. Atomically rename each live file or symbolic link into an operation-specific quarantine
   directory on the same filesystem as its storage root. Verify that each quarantined object is
   the planned object and still corresponds to the verified backup. Never move or modify a
   symbolic link's target.
7. Commit the database transaction only after every file is quarantined and verified.
8. Purge the quarantine after commit. A purge failure leaves cleanup debt outside the live
   namespace; it must not restore files whose database deletion has committed.

Quarantine makes filesystem changes reversible while the database transaction is uncommitted. If
database deletion fails before quarantine, simply roll back. If a quarantine move or verification
fails, restore all paths already moved and roll back. If the database commit is known to have
failed, restore the quarantined paths. An indeterminate commit outcome requires reconciliation
rather than blindly restoring paths.

The quarantine is not a backup: it is temporary, root-local rollback state and is destroyed after
commit. The durable backup may live on another filesystem and remains usable after the operation.
Same-filesystem rename removes the live pathname atomically, but it cannot freeze an inode already
open by another process. Pre/post-copy checks, path and inode checks around rename, and a digest
comparison against the verified backup detect ordinary changes and substitutions; they cannot
prevent writes through an already-open file descriptor. The initial contract therefore assumes
registered imaging resources are immutable during management operations. Stronger guarantees
would require writer coordination, maintenance mode, immutable files, or storage snapshots.

Incoming foreign keys use three operation actions:

- `delete`: the referencing rows are in the selected object's ownership closure;
- `set-null`: the relationship explicitly permits detachment and the request names it with the
  repeatable short-form `--update RELATIONSHIP-PATH` option;
- `block`: the relationship is unknown, was not explicitly requested, is not approved by the
  schema, does not match the reflected live foreign key, or is not nullable.

The schema declares which semantic relationship may be cleared; the CLI request authorizes doing
so for one operation. Nullability alone never grants permission. For example, deleting a DICOM
archive may be planned with:

```bash
loris-data-manager delete \
  --select dicom-archive \
  --where dicom-archive.id=76 \
  --update dicom-archive.mri-upload \
  --update dicom-archive.mri-violation \
  --update dicom-archive.protocol-violated-scan \
  --update dicom-archive.imaging-file
```

This command is a read-only dry-run by default and reports owned rows, files, approved reference
updates, and blockers. Add `--execute` to create the mandatory verified backup and perform the
deletion. The proof of concept accepts exactly one direct `--select dicom-archive` projection;
filters may select one or more archives, while an unnarrowed selection still requires `--all`.

A standalone backup with `--update` remains read-only. It records the intended `set-null` actions
and writes guarded restoration statements using the referencing row's primary key and `FK IS
NULL`. Restoration inserts the archive and its owned rows parent-to-child, then restores surviving
references. Deletion consumes the same plan and performs those `set-null` updates before deleting
the referenced archive. All reflected foreign keys not covered by owned rows or an explicit
requested update remain blockers.

The deletion CLI defaults to dry-run and requires the conspicuous `--execute` flag. Empty or broad
selections retain the existing `--all` guard. For the current testing phase, an expected managed
path or symbolic-link target that is missing produces a warning but does not block backup or
deletion. A missing path is represented explicitly in the backup manifest without a payload;
deletion skips it, but fails if it appears after that capture because the new object was not backed
up. Changed, unexpected, or unsafe paths; mismatched row counts; new blockers; and other
revalidation failures still fail closed. Deletion currently supports a symbolic link only as the
final managed path component. It preserves that link in the backup and moves and purges the link
itself; it never reads, writes, or removes the target. Symbolic links in intermediate path
components remain unsupported because operating below one would cross the configured storage-root
path boundary.

## Design notes

- Known, owned database rows are selected and serialized through their SQLAlchemy ORM tables.
  A follow-up should add a fail-closed preflight check that the columns of every owned ORM table
  match the reflected live database table. This is schema-drift detection, not protection against
  concurrent row changes, and can share comparison code with
  `python/scripts/check_orm_sql_sync.py`.
- Live database reflection remains authoritative for discovering declared foreign-key references,
  including references from deployment-specific tables that have no ORM model.
- Backup and deletion share one explicit database transaction. The operation locks the
  semantically selected anchor rows while serializing them, freezes their returned primary keys as
  the operation selection, and then locks and serializes each owned table in deterministic
  parent-to-child order from those identities. It checks declared incoming references with locking
  reads, writes and verifies the backup while retaining all locks, and may then delete
  child-to-parent before committing. SQLite unit tests exercise planning and transaction boundaries
  but ignore `SELECT FOR UPDATE`; production InnoDB/MariaDB concurrency behavior still needs
  database-backed integration coverage.
- Set-oriented owned database-row collections must be protected by a reflected foreign key from
  the owned row back to the already locked anchor. Planning fails closed when the resource binding
  and live constraint do not agree; this constraint is what prevents closure membership from being
  expanded concurrently while its tables are acquired in sequence.
- The fixed anchor identities are currently retained in application memory. This keeps the first
  deletion slice simple and is appropriate when a small number of logical objects owns many
  physical rows. Revisit the representation, operation limits, or an identity spool only if
  selecting very large numbers of logical anchor objects becomes a concrete requirement.
- Database row locks do not protect local files. File copying detects changes during a copy;
  deletion rehashes real regular files against the verified backup and moves them to the
  operation-specific quarantine before commit. Symbolic links are backed up shallowly and
  revalidated by filesystem identity and link text before the link itself is quarantined.
- The proof of concept treats every surfaced commit failure as an indeterminate outcome and leaves
  quarantine in place for reconciliation; it does not yet distinguish a database-confirmed
  rollback that would permit automatic path restoration.
- The current `mri-violation` kind represents one `mri_violations_log` check failure, not one
  underlying scan. Several such rows can name the same file, and warning rows can name a file
  already owned by an `imaging-file`. Its current `file` ownership binding is therefore too strong.
  Do not permit deletion of `mri-violation` objects until scan/file identity and shared ownership
  are modeled explicitly. This does not prevent DICOM archive removal from setting the violation
  rows' nullable `TarchiveID` references to `NULL`, because those rows and files remain in place.
