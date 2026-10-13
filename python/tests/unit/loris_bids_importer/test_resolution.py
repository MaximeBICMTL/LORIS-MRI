from dataclasses import dataclass
from pathlib import Path

import pytest
from loris_bids_importer.config import (
    BidsCandidateConfig,
    BidsConfigFunctions,
    BidsSessionConfig,
    CreateBidsCandidateConfig,
    CreateBidsSessionConfig,
)
from loris_bids_importer.resolution.candidates import resolve_bids_candidate
from loris_bids_importer.resolution.dataset import resolve_bids_mappings
from loris_bids_importer.resolution.sessions import resolve_bids_session
from loris_bids_utils.files.participants import BidsParticipantTsvRow
from loris_bids_utils.files.sessions import BidsSessionTsvRow
from loris_bids_utils.labels import BidsSessionLabel, BidsSubjectLabel, BidsSubjectSessionLabel
from loris_bids_utils.reader import BidsDatasetReader

from lib.db.models.candidate import DbCandidate
from lib.db.models.cohort import DbCohort
from lib.db.models.project import DbProject
from lib.db.models.project_cohort import DbProjectCohort
from lib.db.models.session import DbSession
from lib.db.models.sex import DbSex
from lib.db.models.site import DbSite
from lib.db.models.visit import DbVisit
from lib.db.models.visit_window import DbVisitWindow
from lib.env import Env


@dataclass
class LorisRecords:
    project: DbProject
    site: DbSite
    cohort: DbCohort


@pytest.fixture
def loris_records(env: Env) -> LorisRecords:
    project = DbProject(id=1, name='Test Project', alias='TEST')
    site = DbSite(id=1, name='Test Site', alias='SITE')
    cohort = DbCohort(id=1, name='Control')
    env.db.add_all([
        project,
        site,
        cohort,
        DbProjectCohort(project_id=project.id, cohort_id=cohort.id),
        DbSex(name='Female'),
        DbVisit(id=1, name='Baseline', label='V1'),
        DbVisitWindow(visit_label='V1'),
        DbVisit(id=2, name='Follow-up', label='V2'),
        DbVisitWindow(visit_label='V2'),
    ])
    env.db.flush()
    return LorisRecords(project, site, cohort)


def test_resolve_created_candidate_and_session(env: Env, loris_records: LorisRecords):
    candidate = resolve_bids_candidate(
        env,
        BidsCandidateConfig(
            psc_id='LORIS001',
            create=CreateBidsCandidateConfig(
                registration_project=loris_records.project.alias,
                registration_site=loris_records.site.alias,
                sex='F',
            ),
        ),
    )

    assert candidate.psc_id == 'LORIS001'
    assert candidate.sex == 'Female'

    session = resolve_bids_session(
        env,
        candidate,
        BidsSessionConfig(
            visit_label='V1',
            create=CreateBidsSessionConfig(
                project=loris_records.project.alias,
                site=loris_records.site.alias,
                cohort=loris_records.cohort.name,
            ),
        ),
    )

    assert session.candidate is candidate
    assert session.visit_label == 'V1'
    assert session.visit_number == 1
    assert session.project_id == loris_records.project.id
    assert session.site_id == loris_records.site.id
    assert session.cohort_id == loris_records.cohort.id

    second_session = resolve_bids_session(
        env,
        candidate,
        BidsSessionConfig(
            visit_label='V2',
            create=CreateBidsSessionConfig(
                project=loris_records.project.alias,
                site=loris_records.site.alias,
                cohort=loris_records.cohort.name,
            ),
        ),
    )

    assert second_session.visit_number == 2


def test_resolve_candidate_identifier_mismatch(env: Env, loris_records: LorisRecords):
    candidate = DbCandidate(
        cand_id=123456,
        psc_id='LORIS001',
        registration_project_id=loris_records.project.id,
        registration_site_id=loris_records.site.id,
    )
    env.db.add(candidate)
    env.db.flush()

    with pytest.raises(Exception, match='mismatch'):
        resolve_bids_candidate(env, BidsCandidateConfig(cand_id=123456, psc_id='OTHER'))


def test_resolve_bids_mappings_passes_stable_metadata(
    env: Env,
    loris_records: LorisRecords,
    tmp_path: Path,
):
    candidate = DbCandidate(
        cand_id=123456,
        psc_id='LORIS001',
        registration_project_id=loris_records.project.id,
        registration_site_id=loris_records.site.id,
    )
    session = DbSession(
        candidate=candidate,
        visit_label='V1',
        visit_number=1,
        project_id=loris_records.project.id,
        site_id=loris_records.site.id,
        cohort_id=loris_records.cohort.id,
    )
    env.db.add_all([candidate, session])
    env.db.flush()

    dataset_path = make_bids_dataset(tmp_path)
    bids = BidsDatasetReader(dataset_path, derivatives=False, validate=False)

    def get_candidate_config(
        hook_env: Env,
        subject_label: str,
        participant_row: BidsParticipantTsvRow | None,
    ) -> BidsCandidateConfig:
        assert hook_env is env
        assert subject_label == 'source01'
        assert participant_row is not None
        assert participant_row.data['group'] == 'source-group'
        return BidsCandidateConfig(psc_id='LORIS001')

    def get_session_config(
        hook_env: Env,
        resolved_candidate: DbCandidate,
        subject_label: str,
        session_label: str | None,
        participant_row: BidsParticipantTsvRow | None,
        session_row: BidsSessionTsvRow | None,
    ) -> BidsSessionConfig:
        assert hook_env is env
        assert resolved_candidate is candidate
        assert subject_label == 'source01'
        assert session_label == 'sourcevisit'
        assert participant_row is not None
        assert participant_row.data['group'] == 'source-group'
        assert session_row is not None
        assert session_row.data['timepoint'] == 'baseline'
        return BidsSessionConfig(visit_label='V1')

    config = BidsConfigFunctions(
        get_candidate=get_candidate_config,
        get_session=get_session_config,
    )

    mappings = resolve_bids_mappings(env, bids, config)

    assert mappings.candidates == {'source01': candidate}
    assert mappings.sessions == {
        BidsSubjectSessionLabel(
            BidsSubjectLabel('source01'),
            BidsSessionLabel('sourcevisit'),
        ): session
    }


def make_bids_dataset(tmp_path: Path) -> Path:
    dataset = tmp_path / 'dataset'
    session = dataset / 'sub-source01' / 'ses-sourcevisit' / 'anat'
    session.mkdir(parents=True)

    (dataset / 'dataset_description.json').write_text(
        '{"Name": "Mapping test", "BIDSVersion": "1.8.0"}'
    )
    (dataset / 'participants.tsv').write_text(
        'participant_id\tgroup\nsub-source01\tsource-group\n'
    )
    (dataset / 'sub-source01' / 'sub-source01_sessions.tsv').write_text(
        'session_id\ttimepoint\nses-sourcevisit\tbaseline\n'
    )
    (dataset / 'sub-source01' / 'sub-source01_sessions.json').write_text(
        '{"timepoint": {"Description": "Study time point"}}'
    )
    (session / 'sub-source01_ses-sourcevisit_T1w.nii.gz').touch()
    return dataset
