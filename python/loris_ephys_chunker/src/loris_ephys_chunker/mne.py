import sys
from typing import Protocol, TypedDict

from mne.io.constants import FIFF  # type: ignore


class MneFiff(Protocol):
    """
    Type wrapper around the MNE FIFF object to provide minimal typing.
    """

    def __getattr__(self, name: str) -> int: ...


class MneChannel(TypedDict):
    """
    Type wrapper around the MNE FIFF object to provide minimal typing.
    """

    ch_name: str
    unit: int
    kind: int
    coil_type: int


FIFF: MneFiff


def print_warning(message: str):
    """
    Print a warning in the terminal.
    """

    print(f"WARNING: {message}", file=sys.stderr)


def get_channel_type(channel: MneChannel) -> str | None:
    """
    Get the type of an MNE channel using match statement.
    """

    match channel['kind']:
        # The MEG CTF system clock channel is currently not recognized by MNE.
        case FIFF.FIFFV_MISC_CH if channel['ch_name'].startswith('SCLK'):
            return 'SYSCLOCK'
        case FIFF.FIFFV_EEG_CH:
            return 'EEG'
        case FIFF.FIFFV_MEG_CH:
            return 'MEG'
        case FIFF.FIFFV_REF_MEG_CH:
            return 'REF_MEG'
        case _:
            print_warning(f"Unknown channel type with MNE FIFF ID {type_id}")
            return None


def get_channel_coil_type(channel: MneChannel) -> str | None:
    """
    Get the coil type of an MNE channel.
    """

    coil_type_id = channel['coil_type']
    if coil_type_id not in FIFF_CHANNEL_COIL_TYPES_DICT:
        print_warning(f"Unknown channel coil type with MNE FIFF ID {coil_type_id}")
        return None

    return FIFF_CHANNEL_COIL_TYPES_DICT[coil_type_id]


def get_channel_unit(channel: MneChannel, channel_type: str | None) -> str | None:
    """
    Get the SI symbol of the unit of an MNE channel.
    """

    # The MEG CTF system clock channel is currently not recognized by MNE.
    if channel_type == 'SYSCLOCK':
        return 's'

    match channel['unit']:
        case FIFF.FIFF_UNIT_SEC:
            return 's'
        case FIFF.FIFF_UNIT_V:
            return 'V'
        case FIFF.FIFF_UNIT_T:
            return 'T'
        case FIFF.FIFF_UNIT_T_M:
            return 'T/m'
        case unit_id:
            print_warning(f"Unknown channel unit with MNE FIFF ID {unit_id}")
            return None


def get_channel_display_unit_factor(channel_type: str, channel_unit: str) -> tuple[str | None, float | None]:
    """
    Get the display unit and display factor of a channel based on its type and unit.
    """

    match channel_type, channel_unit:
        case 'SYSCLOCK', 's':
            return 's', 1
        case 'EEG', 'V':
            return 'μV', 1e6
        case _, _:
            print_warning(f"Unknown channel display unit and factor for type {channel_type} and unit {channel_unit}")
            return None, None


# Mapping from the MNE channel type identifiers to their name.
FIFF_CHANNEL_TYPES_DICT: dict[int, str | None] = {
    FIFF.FIFFV_MISC_CH: None,
    FIFF.FIFFV_EEG_CH: 'EEG',
}

# Mapping from the MNE coil type identifiers to their name.
FIFF_CHANNEL_COIL_TYPES_DICT: dict[int, str | None] = {
    FIFF.FIFFV_COIL_NONE: None,
    FIFF.FIFFV_COIL_EEG: 'EEG',
}

# Mapping from the MNE unit identifiers to their SI symbol.
FIFF_CHANNEL_UNITS_DICT: dict[int, str] = {
    # Used in the MEG system clock.
    FIFF.FIFF_UNIT_SEC: 's',
    # Used in EEG and MEG.
    FIFF.FIFF_UNIT_V: 'V',
    # Used in MEG.
    FIFF.FIFF_UNIT_T: 'T',
    FIFF.FIFF_UNIT_T_M: 'T/m',
}
