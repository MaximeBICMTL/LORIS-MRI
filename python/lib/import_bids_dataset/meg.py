from pathlib import Path

from loris_bids_reader.files.scans import BidsScanTsvRow
from loris_bids_reader.meg.data_type import BidsMegAcquisition
from loris_utils.error import group_errors_tuple
from loris_utils.path import add_path_extension

from lib.config import get_eeg_viz_enabled_config
from lib.db.models.session import DbSession
from lib.db.queries.physio_file import try_get_physio_file_with_path
from lib.env import Env
from lib.import_bids_dataset.args import Args
from lib.import_bids_dataset.channels import insert_bids_channels_file
from lib.import_bids_dataset.copy_files import archive_bids_directory, copy_bids_file, get_loris_file_path
from lib.import_bids_dataset.env import BidsImportEnv
from lib.import_bids_dataset.events import insert_events_metadata_file
from lib.import_bids_dataset.events_tsv import insert_bids_events_file
from lib.import_bids_dataset.file_type import get_check_bids_imaging_file_type
from lib.import_bids_dataset.meg_channels import read_meg_channels
from lib.import_bids_dataset.physio import get_check_bids_physio_modality, get_check_bids_physio_output_type
from lib.logging import log, log_warning
from lib.physio.chunking import create_physio_channels_chunks
from lib.physio.events import FileSource
from lib.physio.file import insert_physio_file
from lib.physio.parameters import insert_physio_file_parameter


def import_bids_meg_acquisition(
    env: Env,
    import_env: BidsImportEnv,
    args: Args,
    session: DbSession,
    acquisition: BidsMegAcquisition,
    scan_row: BidsScanTsvRow | None,
):
    # TODO: The file is actually a directory, it should be tared before proceeding to the hash.
    modality, output_type, file_type = group_errors_tuple(
        f"Error while checking database information for MEG acquisition '{acquisition.name}'.",
        lambda: get_check_bids_physio_modality(env, acquisition.data_type.name),
        lambda: get_check_bids_physio_output_type(env, args.type or 'raw'),
        lambda: get_check_bids_imaging_file_type(env, 'ctf'),
        # lambda: get_check_bids_physio_file_hash(env, acquisition),
    )

    loris_file_path = get_loris_file_path(import_env, session, acquisition, acquisition.ctf_path)

    loris_file = try_get_physio_file_with_path(env.db, loris_file_path)
    if loris_file is not None:
        log(env, f"File '{loris_file_path}' is already registered in LORIS. Skipping.")
        import_env.ignored_files_count += 1
        return

    check_bids_meg_metadata_files(env, acquisition)

    physio_file = insert_physio_file(
        env,
        session,
        loris_file_path,
        file_type,
        modality,
        output_type,
        scan_row.get_acquisition_time() if scan_row is not None else None
    )

    # insert_physio_file_parameter(env, physio_file, 'physiological_json_file_blake2b_hash', file_hash)
    for name, value in acquisition.sidecar.data.items():
        insert_physio_file_parameter(env, physio_file, name, value)

    if acquisition.events is not None:
        insert_bids_events_file(env, import_env, physio_file, session, acquisition, acquisition.events)
        if acquisition.events.dictionary is not None:
            insert_events_metadata_file(env, FileSource(physio_file), acquisition.events.dictionary)

    if acquisition.channels is not None:
        insert_bids_channels_file(env, import_env, physio_file, session, acquisition, acquisition.channels)

    if import_env.loris_bids_path is not None:
        copy_bids_meg_files(import_env.loris_bids_path, session, acquisition)

    env.db.commit()

    log(env, f"MEG file succesfully imported with ID: {physio_file.id}.")

    # TODO: Remove the false.
    if get_eeg_viz_enabled_config(env):
        log(env, "Creating visualization chunks...")
        create_physio_channels_chunks(env, physio_file, acquisition.ctf_path)

    read_meg_channels(env, physio_file, acquisition)

    env.db.commit()

    import_env.imported_files_count += 1


def check_bids_meg_metadata_files(env: Env, acquisition: BidsMegAcquisition):
    """
    Check for the presence of BIDS metadata files for the BIDS MEG acquisition and warn the user if
    that is not the case.
    """

    if acquisition.channels is None:
        log_warning(env, f"No channels file found for acquisition '{acquisition.name}'.")

    if acquisition.events is None:
        log_warning(env, f"No events file found for acquisition '{acquisition.name}'.")

    if acquisition.events is not None and acquisition.events.dictionary is not None:
        log_warning(env, f"No events dictionary file found for acquisition '{acquisition.name}'.")


def copy_bids_meg_files(loris_bids_path: Path, session: DbSession, acquisition: BidsMegAcquisition):
    """
    Copy the files of a BIDS MEG acquisition into a LORIS BIDS directory.
    """

    if acquisition.channels is not None:
        copy_bids_file(loris_bids_path, session, acquisition, acquisition.channels.path)

    if acquisition.events is not None:
        copy_bids_file(loris_bids_path, session, acquisition, acquisition.events.path)
        if acquisition.events.dictionary is not None:
            copy_bids_file(loris_bids_path, session, acquisition, acquisition.events.dictionary.path)

    copy_bids_file(loris_bids_path, session, acquisition, acquisition.ctf_path)
