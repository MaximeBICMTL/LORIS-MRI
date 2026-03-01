import re
from abc import ABC
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path

from bids import BIDSLayout, BIDSLayoutIndexer

from loris_bids_reader.files.dataset_description import BidsDatasetDescriptionJsonFile
from loris_bids_reader.files.participants import BidsParticipantsTsvFile, BidsParticipantTsvRow

PYBIDS_IGNORE = ['.git', 'code/', 'log/', 'sourcedata/']
PYBIDS_FORCE_INDEX = [re.compile(r"_annotations\.(tsv|json)$")]


@dataclass
class BidsDatasetReader:
    """
    A hierarchical BIDS dataset reader. This class is a wrapper around PyBIDS that allows to easily
    read a BIDS dataset one directory level at the time.
    """

    layout: BIDSLayout
    """
    The PyBIDS layout object of this BIDS dataset.
    """

    path: Path
    """
    The path of this BIDS dataset.
    """

    def __init__(self, path: Path, validate: bool = True):
        self.path = path
        self.layout = BIDSLayout(
            path,
            validate=validate,
            derivatives=True,
            indexer=BIDSLayoutIndexer(
                ignore=PYBIDS_IGNORE,
                force_index=PYBIDS_FORCE_INDEX,
            ),
        )

    @cached_property
    def dataset_description_file(self) -> BidsDatasetDescriptionJsonFile | None:
        """
        The `dataset_description.json` file of this BIDS dataset, if it exists.
        """

        dataset_description_path = self.path / 'dataset_description.json'
        if not dataset_description_path.is_file():
            return None

        return BidsDatasetDescriptionJsonFile(dataset_description_path)

    @cached_property
    def participants_file(self) -> BidsParticipantsTsvFile | None:
        """
        The `participants.tsv` file of this BIDS dataset, if it exists.
        """

        participants_path = self.path / 'participants.tsv'
        if not participants_path.is_file():
            return None

        return BidsParticipantsTsvFile(participants_path)

    @cached_property
    def subject_labels(self) -> list[str]:
        """
        The subject labels present in this BIDS dataset, without the `sub-` prefix.
        """

        return self.layout.get_subjects()  # type: ignore

    @cached_property
    def session_labels(self) -> list[str]:
        """
        The session labels present in this BIDS dataset, without the `ses-` prefix.
        """

        return self.layout.get_sessions()  # type: ignore

    @cached_property
    def subjects(self) -> list['BidsSubjectReader']:
        """
        Get the subject directory readers of this BIDS dataset.
        """

        return [
            BidsSubjectReader(
                dataset=self,
                path=self.path / f'sub-{subject}',
                subject=subject   # type: ignore
            ) for subject in self.layout.get_subjects()  # type: ignore
        ]

    @cached_property
    def sessions(self) -> list['BidsSessionReader']:
        """
        Get the session directory readers of this BIDS dataset.
        """

        return [
            session_reader
            for subject in self.subjects
            for session_reader in subject.sessions
        ]

    @cached_property
    def data_types(self) -> list['BidsDataTypeReader']:
        """
        Get the data type directory readers of this BIDS dataset.
        """

        return [
            data_type_reader
            for subject_reader in self.subjects
            for data_type_reader in subject_reader.data_types
        ]


@dataclass(frozen=True)
class BidsBaseReader(ABC):
    """
    Abstract base class for a BIDS directory or sub-directory reader.
    """

    dataset: BidsDatasetReader
    """
    The dataset reader of the BIDS dataset to which this directory belongs to.
    """

    path: Path
    """
    The path of this directory.
    """


@dataclass(frozen=True)
class BidsSubjectBaseReader(BidsBaseReader):
    """
    A BIDS subject directory or sub-directory reader.
    """

    subject: str
    """
    The subject label of this directory, without the `sub-` prefix.
    """

    @cached_property
    def participant_row(self) -> BidsParticipantTsvRow | None:
        """
        The row of the `participants.tsv` file corresponding to this subject, if it exists.
        """

        if self.dataset.participants_file is None:
            return None

        return self.dataset.participants_file.get_row(self.subject)


@dataclass(frozen=True)
class BidsSubjectReader(BidsSubjectBaseReader):
    """
    A BIDS subject directory reader.
    """

    @cached_property
    def sessions(self) -> list['BidsSessionReader']:
        """
        Get the session directory readers of this subject.
        """

        return [
            BidsSessionReader(
                dataset=self.dataset,
                path=self.path / f'ses-{session}',  # type: ignore
                subject=self.subject,
                session=session   # type: ignore
            ) for session in self.dataset.layout.get_sessions(subject=self.subject)  # type: ignore
        ]

    @cached_property
    def data_types(self) -> list['BidsDataTypeReader']:
        """
        Get the data type directory readers of this subject.
        """

        if self.sessions == []:
            return [
                BidsDataTypeReader(
                    dataset=self.dataset,
                    path=self.path / data_type,  # type: ignore
                    subject=self.subject,
                    session=None,
                    data_type=data_type   # type: ignore
                ) for data_type in self.dataset.layout.get_datatypes(subject=self.subject)  # type: ignore
            ]

        return [
            data_type_reader
            for session_reader in self.sessions
            for data_type_reader in session_reader.data_types
        ]


@dataclass(frozen=True)
class BidsSessionBaseReader(BidsSubjectBaseReader):
    """
    A BIDS session directory or sub-directory reader.
    """

    session: str | None
    """
    The session label of this directory if there is one, without the `ses-` prefix.
    """


@dataclass(frozen=True)
class BidsSessionReader(BidsSessionBaseReader):
    """
    A BIDS session directory reader.
    """

    session: str
    """
    The session label of this session, without the `ses-` prefix.
    """

    @cached_property
    def data_types(self) -> list['BidsDataTypeReader']:
        """
        Get the data type directory readers of this session.
        """

        return [
            BidsDataTypeReader(
                dataset=self.dataset,
                path=self.path / data_type,  # type: ignore
                subject=self.subject,
                session=self.session,
                data_type=data_type   # type: ignore
            ) for data_type in self.dataset.layout.get_datatypes(   # type: ignore
                subject=self.subject,
                session=self.session,
            )
        ]


@dataclass(frozen=True)
class BidsDataTypeBaseReader(BidsSessionBaseReader):
    """
    A BIDS data type directory or sub-directory reader.
    """

    data_type: str
    """
    The data type name of this data type directory.
    """


@dataclass(frozen=True)
class BidsDataTypeReader(BidsDataTypeBaseReader):
    """
    A BIDS data type directory reader.
    """

    pass
