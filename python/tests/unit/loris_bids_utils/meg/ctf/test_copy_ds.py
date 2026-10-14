from pathlib import Path

import pytest
from loris_bids_utils.meg.ctf import copy_ds


def test_copy_meg_ctf_ds_updates_dataset_files(tmp_path: Path):
    source = tmp_path / 'old.ds'
    destination = tmp_path / 'new.ds'
    source.mkdir()
    (source / 'old_old.meg4').write_bytes(b'MEG data')
    (source / 'old.hist').write_text('opened old.ds\n', encoding='latin-1')
    (source / 'old.cls').write_bytes(b'class\r\nold.ds\r\nold.ds\r\n')
    (source / 'old.mrk').write_bytes(b'marker\r\nold.ds\r\nold.ds\r\n')
    (source / 'processing.cfg.bak').touch()
    (source / 'old.eeg').touch()
    (source / 'mystery.bin').write_bytes(b'unknown')
    source_before = {
        path.name: path.read_bytes()
        for path in source.iterdir()
        if path.is_file()
    }

    with pytest.warns(UserWarning):
        unknown_files = copy_ds.copy_meg_ctf_ds(source, destination)

    assert unknown_files == [Path('mystery.bin')]
    assert (destination / 'new_old.meg4').read_bytes() == b'MEG data'
    assert (destination / 'new.hist').read_text(encoding='latin-1') == 'opened new.ds\n'
    assert (destination / 'new.cls').read_bytes() == b'class\r\nnew.ds\r\nold.ds\r\n'
    assert (destination / 'new.mrk').read_bytes() == b'marker\r\nnew.ds\r\nold.ds\r\n'
    assert not (destination / 'processing.cfg.bak').exists()
    assert not (destination / 'new.eeg').exists()
    assert {
        path.name: path.read_bytes()
        for path in source.iterdir()
        if path.is_file()
    } == source_before


def test_copy_meg_ctf_ds_preserves_history_without_dataset_name(tmp_path: Path):
    source = tmp_path / 'old.ds'
    destination = tmp_path / 'new.ds'
    source.mkdir()
    history = b'Collection started\r\nNo dataset reference here\r\n'
    (source / 'old.hist').write_bytes(history)

    copy_ds.copy_meg_ctf_ds(source, destination)

    assert (destination / 'new.hist').read_bytes() == history


def test_copy_meg_ctf_ds_rejects_nonempty_destination(tmp_path: Path):
    source = tmp_path / 'source.ds'
    destination = tmp_path / 'destination.ds'
    source.mkdir()
    destination.mkdir()
    (destination / 'stale.meg4').touch()

    with pytest.raises(FileExistsError, match="not empty"):
        copy_ds.copy_meg_ctf_ds(source, destination)


def test_copy_meg_ctf_ds_rejects_destination_inside_source(tmp_path: Path):
    source = tmp_path / 'source.ds'
    source.mkdir()

    with pytest.raises(ValueError, match="inside the source"):
        copy_ds.copy_meg_ctf_ds(source, source / 'nested.ds')


def test_copy_bids_meg_ctf_ds_updates_new_subject_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    source = tmp_path / 'sub-old_task-rest_meg.ds'
    destination = tmp_path / 'sub-new_task-rest_meg.ds'
    source.mkdir()
    (source / 'sub-old_task-rest_meg.meg4').write_bytes(b'MEG data')
    updated_subjects: list[tuple[Path, str]] = []

    def record_subject_update(ds_path: Path, subject: str):
        updated_subjects.append((ds_path, subject))

    monkeypatch.setattr(
        copy_ds,
        '_replace_subject_metadata',
        record_subject_update,
    )

    copy_ds.copy_bids_meg_ctf_ds(source, destination)

    assert updated_subjects == [(destination, 'sub-new')]
    assert (destination / 'sub-new_task-rest_meg.meg4').exists()
