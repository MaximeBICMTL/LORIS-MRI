
import os
import shutil
from pathlib import Path

from loris_bids_reader.files.scans import BidsScansTsvFile

import lib.utilities
from lib.db.models.session import DbSession
from lib.import_bids_dataset.info import BidsImportInfo


def get_bids_file_full_path(
    info: BidsImportInfo,
    session: DbSession,
    data_type: str,
    file_path: Path,
    derivative: bool = False,
) -> Path:
    """
    Get the full path of a BIDS file for its import into LORIS.
    """

    # In the import is run in no-copy mode, simply return the original file path.
    if info.loris_bids_path is None:
        return file_path

    # If the file is a derivative, since the path is unpredictable, return a copy of that path in
    # the LORIS BIDS dataset.
    if derivative:
        return info.loris_bids_path / file_path.relative_to(info.source_bids_path)

    # Otherwise, normalize the subject and session directrory names using the LORIS session
    # information.
    return (
        info.loris_bids_path
        / f'sub-{session.candidate.psc_id}'
        / f'ses-{session.visit_label}'
        / data_type
        / file_path.name
    )


def copy_bids_file(
    info: BidsImportInfo,
    session: DbSession,
    data_type: str,
    file_path: Path,
    derivative: bool = False,
) -> Path:
    """
    Copy a BIDS file into the LORIS data directory, unless the no-copy mode is enabled. Return the
    path of that file relative to the LORIS data directory.
    """

    loris_file_full_path = get_bids_file_full_path(info, session, data_type, file_path, derivative)
    loris_file_path = loris_file_full_path.relative_to(info.data_dir_path)

    if info.loris_bids_path is None:
        return loris_file_path

    if loris_file_full_path.exists():
        raise Exception(f"File '{loris_file_path}' already exists in LORIS.")

    loris_file_full_path.parent.mkdir(parents=True, exist_ok=True)
    if file_path.is_file():
        shutil.copyfile(file_path, loris_file_full_path)
    elif file_path.is_dir():
        shutil.copytree(file_path, loris_file_full_path)

    return loris_file_path


def copy_scans_tsv_file_to_loris_bids_dir(
    scans_file: BidsScansTsvFile,
    bids_sub_id: str,
    loris_bids_root_dir: str,
    data_dir: str,
) -> str:
    """
    Copy the scans.tsv file to the LORIS BIDS directory for the subject.
    """

    original_file_path = scans_file.path
    final_file_path = os.path.join(loris_bids_root_dir, f'sub-{bids_sub_id}', scans_file.path.name)

    # copy the scans.tsv file to the new directory
    if os.path.exists(final_file_path):
        lib.utilities.append_to_tsv_file(original_file_path, final_file_path, 'filename', False)  # type: ignore
    else:
        lib.utilities.copy_file(original_file_path, final_file_path, False)  # type: ignore

    # determine the relative path and return it
    return os.path.relpath(final_file_path, data_dir)
