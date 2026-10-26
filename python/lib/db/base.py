from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass


class Base(MappedAsDataclass, DeclarativeBase, kw_only=True, eq=False, repr=False):
    """
    Base SQLAlchemy class that must be inherited by all the ORM model classes.

    Models also declare ``kw_only=True`` and ``eq=False`` explicitly so type checkers recognize
    keyword-only fields and identity-based equality and hashing.
    """

    # Make identity hashing explicit for type checkers as well as at runtime.
    def __hash__(self) -> int:
        return object.__hash__(self)
