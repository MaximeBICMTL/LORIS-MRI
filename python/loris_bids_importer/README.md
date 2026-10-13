# LORIS BIDS importer

## Description

The LORIS BIDS importer is a pipeline that allows to ingest an external BIDS dataset into LORIS, populating both the database and the file system.

The LORIS BIDS importer supports the MRI (NIfTI), EEG/iEEG (EDF/EEGLAB), and MEG (CTF) modalities.

## Installation

This package is installed by default with LORIS Python.

## Script

The LORIS BIDS importer can be run using the following command:

```sh
import-bids-dataset -d /path/to/bids/dataset
```

The directory passed as an argument should be a BIDS compliant dataset (see the [BIDS specification](https://bids-specification.readthedocs.io/en/stable/)).

## Parameters

### Subject and session resolution

By default, every BIDS subject label must correspond to a LORIS CandID or PSCID, and every BIDS session label must correspond to a LORIS visit label. The candidates and sessions of the BIDS dataset must already exist in LORIS when running the import.

The `--bids-config` argument can be used to provide a custom configuration file that allows to:
- Import a sessionless BIDS dataset.
- Use an arbitrary mapping between BIDS and LORIS subjects/candidates and sessions.
- Create missing LORIS candidates and sessions using the BIDS metadata.

```sh
import-bids-dataset --directory /path/to/bids/dataset --bids-config /path/to/bids_import_config.py
```

The Python configuration file must define two functions:

```python
from lib.db.models.candidate import DbCandidate
from lib.env import Env
from loris_bids_utils.files.participants import BidsParticipantTsvRow
from loris_bids_utils.files.sessions import BidsSessionTsvRow

from loris_bids_importer.config import (
    BidsCandidateConfig,
    BidsSessionConfig,
    CreateBidsCandidateConfig,
    CreateBidsSessionConfig,
)


def get_bids_candidate_config(
    env: Env,
    subject_label: str,
    participant_row: BidsParticipantTsvRow | None,
) -> BidsCandidateConfig:
    return BidsCandidateConfig(
        psc_id=f"PROJECT{int(subject_label):06}",
        create=CreateBidsCandidateConfig(
            registration_project="PROJECT",
            registration_site="SITE",
            sex=participant_row.sex if participant_row is not None else None,
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
    visit_labels = {
        None: "V1",
        "baseline": "V1",
        "followup": "V2",
    }

    return BidsSessionConfig(
        visit_label=visit_labels[session_label],
        create=CreateBidsSessionConfig(
            project="PROJECT",
            site="SITE",
            cohort="Control",
        ),
    )
```

The `project` and `site` strings may be names or aliases, the `create` argument is optional and should be provided only to create missing LORIS candidates and sessions. The configuration code may raise an exception if the provided BIDS information is incomplete or incorrect.

### File copy

By default, the files imported from the source BIDS dataset are copied in a new BIDS dataset in the LORIS data directory. The `--no-copy` option allows to re-use the original BIDS files without copy if the source BIDS dataset is already located in the LORIS data directory.

### Other parameters

To see all the CLI parameters of the LORIS BIDS importer, use `import-bids-dataset --help`.

## Configuration

### MRI

The LORIS BIDS importer assigns LORIS scan types to MRI files using the provided BIDS suffixes. For instance, if a NIfTI file is named `sub-ABC000123_ses-ABC_T1map.nii.gz`, it will be assigned the LORIS `T1map` scan type.

The LORIS BIDS importer automatically creates standard BIDS scan types if they do not exist in the LORIS database yet. If a project uses custom BIDS scan types, these should be added in the `mri_scan_type` table beforehand.

### EEG/iEEG/MEG

To be visualized in the LORIS electrophysiology browser, EEG/iEEG/MEG files must be pre-processed by the [LORIS electrophysiology chunker](../loris_ephys_chunker). To enable pre-processing of electrophysiology files during BIDS import, the LORIS `useEEGBrowserVisualizationComponents` configuration value must be set to `true`.

## Supported files

The LORIS BIDS importer can import the following BIDS files:
- Static dataset files: `dataset_description.json`, `README`.
- Dataset structure files: `participants.tsv`, `sessions.tsv`, `scans.tsv`.
- MRI acquisition files: NIfTI (`.nii`, `.nii.gz`) and their JSON sidecars.
- EEG/iEEG/MEG acquisition files: EDF (`.edf`), EEGLAB (`.set`), CTF (`.ds` directories), and their JSON sidecars.
- EEG/iEEG/MEG metadata files: `coordsystem.json`, `electrodes.tsv`, `channels.tsv`, `events.json`, `events.tsv`.
