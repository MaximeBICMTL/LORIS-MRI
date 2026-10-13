from dataclasses import dataclass
from typing import cast

from lib.db.models.candidate import DbCandidate
from lib.db.models.session import DbSession
from lib.env import Env
from loris_bids_utils.labels import BidsSubjectLabel, BidsSubjectSessionLabel
from loris_bids_utils.reader import BidsDatasetReader, BidsSessionReader, BidsSubjectReader
from loris_utils.error import group_errors

from loris_bids_importer.config import (
    BidsCandidateConfig,
    BidsConfigError,
    BidsConfigFunctions,
    BidsSessionConfig,
)
from loris_bids_importer.resolution.candidates import resolve_bids_candidate
from loris_bids_importer.resolution.sessions import resolve_bids_session


@dataclass
class BidsLorisMappings:
    """
    Mappings from BIDS subject and session labels to LORIS candidates and sessions.
    """

    candidates: dict[BidsSubjectLabel, DbCandidate]
    sessions: dict[BidsSubjectSessionLabel, DbSession]


def resolve_bids_mappings(env: Env, bids: BidsDatasetReader, config: BidsConfigFunctions) -> BidsLorisMappings:
    """
    Configure and resolve the LORIS candidate and session mappings of a BIDS dataset.

    Database changes are flushed but not committed. Any error rolls back all changes performed by
    this resolution.
    """

    try:
        return _resolve_bids_mappings(env, bids, config)
    except BaseException:
        env.db.rollback()
        raise


def _resolve_bids_mappings(env: Env, bids: BidsDatasetReader, config: BidsConfigFunctions) -> BidsLorisMappings:
    candidate_configs = group_errors(
        "Could not configure the LORIS candidates for the BIDS dataset.",
        (
            lambda: get_bids_candidate_config(env, config, bids_subject)
            for bids_subject in bids.subjects
        ),
    )

    candidates = group_errors(
        "Could not get or create the LORIS candidates for the BIDS dataset.",
        (
            lambda: _resolve_bids_candidate(
                env,
                bids_subject,
                candidate_config,
            )
            for bids_subject, candidate_config in zip(
                bids.subjects, candidate_configs, strict=True
            )
        ),
    )

    candidate_mappings = {
        bids_subject.label: candidate
        for bids_subject, candidate in zip(bids.subjects, candidates, strict=True)
    }

    session_configs = group_errors(
        "Could not configure the LORIS sessions for the BIDS dataset.",
        (
            lambda: get_bids_session_config(
                env,
                config,
                bids_session,
                candidate_mappings[bids_session.subject.label],
            )
            for bids_session in bids.sessions
        ),
    )

    sessions = group_errors(
        "Could not get or create the LORIS sessions for the BIDS dataset.",
        (
            lambda: _resolve_bids_session(
                env,
                bids_session,
                candidate_mappings[bids_session.subject.label],
                session_config,
            )
            for bids_session, session_config in zip(
                bids.sessions, session_configs, strict=True
            )
        ),
    )

    session_mappings = {
        bids_session.subject_session_label: session
        for bids_session, session in zip(bids.sessions, sessions, strict=True)
    }

    return BidsLorisMappings(candidate_mappings, session_mappings)


def get_bids_candidate_config(
    env: Env,
    config: BidsConfigFunctions,
    bids_subject: BidsSubjectReader,
) -> BidsCandidateConfig:
    """
    Call and validate the candidate configuration function for a BIDS subject.
    """

    try:
        candidate_config = cast(
            object,
            config.get_candidate(env, bids_subject.label, bids_subject.participant_row),
        )
    except Exception as error:
        raise BidsConfigError(
            f"Could not get candidate configuration for BIDS subject '{bids_subject.label}': {error}"
        ) from error

    if not isinstance(candidate_config, BidsCandidateConfig):
        raise BidsConfigError(
            f"Candidate configuration function returned '{type(candidate_config).__name__}' for BIDS subject"
            f" '{bids_subject.label}', expected `BidsCandidateConfig`."
        )

    return candidate_config


def get_bids_session_config(
    env: Env,
    config: BidsConfigFunctions,
    bids_session: BidsSessionReader,
    candidate: DbCandidate,
) -> BidsSessionConfig:
    """
    Call and validate the session configuration function for a BIDS session.
    """

    name = bids_session.subject_session_label.display_name

    try:
        session_config = cast(
            object,
            config.get_session(
                env,
                candidate,
                bids_session.subject.label,
                bids_session.label,
                bids_session.subject.participant_row,
                bids_session.session_row,
            ),
        )
    except Exception as error:
        raise BidsConfigError(f"Could not get session configuration for BIDS session '{name}': {error}") from error

    if not isinstance(session_config, BidsSessionConfig):
        raise BidsConfigError(
            f"Session configuration function returned '{type(session_config).__name__}' for BIDS session"
            f" '{name}', expected `BidsSessionConfig`."
        )

    return session_config


def _resolve_bids_candidate(env: Env, bids_subject: BidsSubjectReader, config: BidsCandidateConfig) -> DbCandidate:
    try:
        return resolve_bids_candidate(env, config)
    except Exception as error:
        raise Exception(
            f"Could not resolve BIDS subject '{bids_subject.label}': {error}"
        ) from error


def _resolve_bids_session(
    env: Env,
    bids_session: BidsSessionReader,
    candidate: DbCandidate,
    config: BidsSessionConfig,
) -> DbSession:
    try:
        return resolve_bids_session(env, candidate, config)
    except Exception as error:
        name = bids_session.subject_session_label.display_name
        raise Exception(f"Could not resolve BIDS session '{name}': {error}") from error
