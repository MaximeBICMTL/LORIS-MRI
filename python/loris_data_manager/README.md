# LORIS data manager

Experimental logical resource model and data-management tools for LORIS.

The package currently provides a read-only resource inspector. Its API is unstable while the
resource model and operation semantics are developed through concrete vertical slices.

## Design notes

- Known, owned database rows are selected and serialized through their SQLAlchemy ORM tables.
  Before deletion is implemented, consider adding a fail-closed preflight check that the columns
  of every owned ORM table match the reflected live database table. This is schema-drift
  detection, not protection against concurrent row changes, and can be implemented later by
  sharing comparison code with `python/scripts/check_orm_sql_sync.py`.
- Live database reflection remains authoritative for discovering declared foreign-key references,
  including references from deployment-specific tables that have no ORM model.
- Backup and future deletion must share one explicit database transaction. The operation locks the
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
- Database row locks do not protect local files. File copying currently detects changes during a
  copy, but a future deletion implementation must define revalidation or quarantine behavior for
  changes between backup and filesystem removal, including explicit symlink semantics.
