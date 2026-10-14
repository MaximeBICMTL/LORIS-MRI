import pytest
from loris_bids_utils.meg.ctf.cpersist import (
    END_MARKER,
    START_MARKER,
    CPersistFormatError,
    CPersistLabel,
    CPersistReader,
    CPersistWriter,
    ValueType,
    replace_strings,
)


def _tag(label: str, value_type: ValueType, value: bytes = b'') -> bytes:
    encoded_label = label.encode('ascii')
    return (
        len(encoded_label).to_bytes(4, 'big', signed=True)
        + encoded_label
        + int(value_type).to_bytes(4, 'big', signed=True)
        + value
    )


def _cstring(value: str) -> bytes:
    encoded = value.encode('latin-1')
    return len(encoded).to_bytes(4, 'big', signed=True) + encoded


def _document(*tags: bytes) -> bytes:
    encoded_end = END_MARKER.encode('ascii')
    return START_MARKER + b''.join(tags) + len(encoded_end).to_bytes(4, 'big', signed=True) + encoded_end


def test_reader_reports_offset_for_truncated_value():
    reader = CPersistReader(b'\x00\x01')

    with pytest.raises(CPersistFormatError, match=r"byte 0.*only 2 remain"):
        reader.read_int32("test integer")


def test_writer_writes_big_endian_int32():
    writer = CPersistWriter()
    writer.write_int32(0x01020304)

    assert writer.to_bytes() == b'\x01\x02\x03\x04'


def test_reader_and_writer_handle_length_prefixed_labels():
    encoded_label = b'\x00\x00\x00\x05value'
    reader = CPersistReader(encoded_label)
    writer = CPersistWriter()

    label = reader.read_label()
    writer.write_label(label)

    assert label == CPersistLabel('value')
    assert writer.to_bytes() == encoded_label


def test_replace_strings_replaces_selected_cstring():
    content = _document(
        _tag('_PATIENT_ID', ValueType.CSTRING, _cstring('old-subject')),
        _tag('unchanged', ValueType.CSTRING, _cstring('original')),
    )

    updated = replace_strings(content, {'_PATIENT_ID': 'sub-new'})

    assert updated == _document(
        _tag('_PATIENT_ID', ValueType.CSTRING, _cstring('sub-new')),
        _tag('unchanged', ValueType.CSTRING, _cstring('original')),
    )


def test_replace_strings_validates_all_replacements_before_parsing():
    with pytest.raises(ValueError, match=r"must be Latin-1 for 'unused'"):
        replace_strings(b'not a CPersist object', {'unused': '\N{SNOWMAN}'})


@pytest.mark.parametrize(
    ('value_type', 'value'),
    [
        (ValueType.DOUBLE, bytes(8)),
        (ValueType.INTEGER, bytes(4)),
        (ValueType.SHORT, bytes(2)),
        (ValueType.BOOLEAN_CHAR, bytes(1)),
        (ValueType.CSTR32, bytes(32)),
        (ValueType.BINARY, (3).to_bytes(4, 'big', signed=True) + b'abc'),
        (
            ValueType.CSTRING_LIST,
            (2).to_bytes(4, 'big', signed=True) + _cstring('one') + _cstring('two'),
        ),
        (ValueType.CSTR32_LIST, (2).to_bytes(4, 'big', signed=True) + bytes(64)),
        (
            ValueType.SENSOR_CLASS_LIST,
            (2).to_bytes(4, 'big', signed=True) + bytes(8),
        ),
    ],
)
def test_replace_strings_preserves_other_value_types(
    value_type: ValueType,
    value: bytes,
):
    content = _document(_tag('value', value_type, value))

    assert replace_strings(content, {}) == content


def test_replace_strings_handles_nested_object():
    encoded_end = END_MARKER.encode('ascii')
    end = len(encoded_end).to_bytes(4, 'big', signed=True) + encoded_end
    content = _document(
        _tag('nested', ValueType.CPERSIST),
        START_MARKER,
        _tag('_PATIENT_ID', ValueType.CSTRING, _cstring('old-subject')),
        end,
    )

    updated = replace_strings(content, {'_PATIENT_ID': 'sub-new'})

    assert updated == content.replace(_cstring('old-subject'), _cstring('sub-new'))


def test_replace_strings_requires_start_marker_after_nested_object_tag():
    content = _document(
        _tag('nested', ValueType.CPERSIST),
        _tag('value', ValueType.INTEGER, bytes(4)),
    )

    with pytest.raises(CPersistFormatError, match=r"Expected b'WS1_'"):
        replace_strings(content, {})


def test_replace_strings_rejects_missing_start_marker():
    with pytest.raises(CPersistFormatError, match=r"Expected b'WS1_'.*byte 0"):
        replace_strings(_tag('value', ValueType.INTEGER, bytes(4)), {})


def test_replace_strings_rejects_unknown_value_type_with_offset():
    label = b'value'
    content = START_MARKER + len(label).to_bytes(4, 'big', signed=True) + label + (99).to_bytes(4, 'big', signed=True)

    with pytest.raises(CPersistFormatError, match=r"Unknown value type 99.*byte 13"):
        replace_strings(content, {})


def test_replace_strings_rejects_unclosed_object():
    with pytest.raises(CPersistFormatError, match="Unclosed CPersist object"):
        replace_strings(START_MARKER, {})


def test_replace_strings_rejects_truncated_value_before_returning_output():
    content = START_MARKER + _tag('value', ValueType.DOUBLE, bytes(7))

    with pytest.raises(CPersistFormatError, match="Truncated value for 'value'"):
        replace_strings(content, {})
