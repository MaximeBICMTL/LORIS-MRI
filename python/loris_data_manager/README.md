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
