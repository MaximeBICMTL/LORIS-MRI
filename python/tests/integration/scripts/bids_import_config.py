from loris_bids_importer.config import (
    BidsCandidateConfig,
    BidsSessionConfig,
    CreateBidsCandidateConfig,
    CreateBidsSessionConfig,
)
from loris_bids_utils.files.participants import BidsParticipantTsvRow
from loris_bids_utils.files.sessions import BidsSessionTsvRow

from lib.db.models.candidate import DbCandidate
from lib.env import Env


def get_bids_candidate_config(
    env: Env,
    subject_label: str,
    participant_row: BidsParticipantTsvRow | None,
) -> BidsCandidateConfig:
    if participant_row is None:
        raise Exception(f"Missing participants.tsv row for subject '{subject_label}'.")

    if participant_row.project is None or participant_row.site is None:
        raise Exception(f"Missing project or site for subject '{subject_label}'.")

    return BidsCandidateConfig(
        psc_id=subject_label,
        create=CreateBidsCandidateConfig(
            registration_project=participant_row.project,
            registration_site=participant_row.site,
            sex=participant_row.sex,
            date_of_birth=participant_row.birth_date,
        ),
    )


def get_bids_session_config(
    env: Env,
    candidate: DbCandidate,
    subject_label: str,
    session_label: str | None,
    participant_row: BidsParticipantTsvRow | None,
    session_row: BidsSessionTsvRow | None,
) -> BidsSessionConfig:
    if session_label is None:
        raise Exception(f"Missing session label for subject '{subject_label}'.")

    if participant_row is None:
        raise Exception(f"Missing participants.tsv row for subject '{subject_label}'.")

    if participant_row.project is None or participant_row.site is None or participant_row.cohort is None:
        raise Exception(f"Missing project, site, or cohort for subject '{subject_label}'.")

    return BidsSessionConfig(
        visit_label=session_label,
        create=CreateBidsSessionConfig(
            project=participant_row.project,
            site=participant_row.site,
            cohort=participant_row.cohort,
        ),
    )
