from pathlib import Path

import pytest
from loris_bids_utils.meg.ctf.res4 import (
    MINIMUM_RES4_SIZE,
    RES4_HEADER,
    SUBJECT_ID_OFFSET,
    SUBJECT_ID_SIZE,
    encode_subject,
    replace_subject,
)


def _write_res4(path: Path, header: bytes = RES4_HEADER) -> None:
    content = bytearray(MINIMUM_RES4_SIZE)
    content[: len(header)] = header
    path.write_bytes(content)


def test_encode_subject_pads_field_with_null_bytes():
    assert encode_subject('sub-123') == b'sub-123' + bytes(SUBJECT_ID_SIZE - 7)


def test_encode_subject_rejects_value_larger_than_field():
    with pytest.raises(ValueError, match="limited to 32 bytes"):
        encode_subject('x' * (SUBJECT_ID_SIZE + 1))


def test_replace_subject_updates_documented_field(tmp_path: Path):
    res4_path = tmp_path / 'recording.res4'
    _write_res4(res4_path)
    original = res4_path.read_bytes()

    replace_subject(tmp_path, 'sub-123')

    content = res4_path.read_bytes()
    assert content[:SUBJECT_ID_OFFSET] == original[:SUBJECT_ID_OFFSET]
    assert content[SUBJECT_ID_OFFSET : SUBJECT_ID_OFFSET + SUBJECT_ID_SIZE] == encode_subject('sub-123')
    assert content[SUBJECT_ID_OFFSET + SUBJECT_ID_SIZE :] == original[SUBJECT_ID_OFFSET + SUBJECT_ID_SIZE :]


def test_replace_subject_rejects_truncated_file(tmp_path: Path):
    res4_path = tmp_path / 'recording.res4'
    res4_path.write_bytes(RES4_HEADER)

    with pytest.raises(ValueError, match="shorter than"):
        replace_subject(tmp_path, 'sub-123')


def test_replace_subject_rejects_unsupported_header(tmp_path: Path):
    res4_path = tmp_path / 'recording.res4'
    _write_res4(res4_path, b'NOTRES4\x00')

    with pytest.raises(ValueError, match="Unsupported CTF res4 header"):
        replace_subject(tmp_path, 'sub-123')


def test_replace_subject_validates_all_files_before_writing(tmp_path: Path):
    valid_path = tmp_path / 'a.res4'
    invalid_path = tmp_path / 'b.res4'
    _write_res4(valid_path)
    _write_res4(invalid_path, b'NOTRES4\x00')
    original = valid_path.read_bytes()

    with pytest.raises(ValueError, match="Unsupported CTF res4 header"):
        replace_subject(tmp_path, 'sub-123')

    assert valid_path.read_bytes() == original
