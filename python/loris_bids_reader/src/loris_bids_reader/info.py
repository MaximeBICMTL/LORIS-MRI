from dataclasses import dataclass


@dataclass
class BidsSubjectInfo:
    """
    Information about a BIDS subject directory.
    """

    subject: str
    """
    The BIDS subject label.
    """


@dataclass
class BidsSessionInfo(BidsSubjectInfo):
    """
    Information about a BIDS session directory.
    """

    session: str | None
    """
    The BIDS session label.
    """


@dataclass
class BidsDataTypeInfo(BidsSessionInfo):
    """
    Information about a BIDS data type directory.
    """

    data_type: str
    """
    The BIDS data type name.
    """
