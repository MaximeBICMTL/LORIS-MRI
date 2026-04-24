from dataclasses import dataclass
from typing import Any


@dataclass
class AcquisitionMedatadata:
    """
    Metadata about an electrophysiology acquisition.
    """

    chunk_size    : int
    signal_range  : tuple[float, float]
    time_interval : tuple[float, float]
    downsamplings : list[int]
    valid_samples : list[int]
    shapes        : list[list[int]]
    trace_types   : dict[Any, Any]
    channels      : 'list[ChannelMetadata]'

    def to_dict(self) -> dict[str, Any]:
        return {
            'chunkSize'     : self.chunk_size,
            'seriesRange'   : self.signal_range,
            'timeInterval'  : self.time_interval,
            'downsamplings' : self.downsamplings,
            'validSamples'  : self.valid_samples,
            'shapes'        : self.shapes,
            'traceTypes'    : self.trace_types,
            'channels'      : [channel.to_dict() for channel in self.channels]
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> 'AcquisitionMedatadata':
        return AcquisitionMedatadata(
            chunk_size    = data['chunkSize'],
            signal_range  = tuple(data['seriesRange']),
            time_interval = tuple(data['timeInterval']),
            downsamplings = data['downsamplings'],
            valid_samples = data['validSamples'],
            shapes        = data['shapes'],
            trace_types   = data['traceTypes'],
            channels      = [ChannelMetadata.from_dict(channel) for channel in data['channels']]
        )


@dataclass
class ChannelMetadata:
    """
    Metadata about an electrophysiology channel.
    """

    name: str
    """
    The name of this channel.
    """

    index: int
    """
    The index of this channel.
    """

    signal_range: tuple[float, float]
    """
    The singal range of this channel.
    """

    type: str | None = None
    """
    The type of this channel.
    """

    coil_type: str | None = None
    """
    The coil type of this channel.
    """

    unit: str | None = None
    """
    The raw unit of the signal values of this channel.
    """

    display_unit: str | None = None
    """
    The unit that should be used to display the signal values in the elctrophysiology browser.
    """

    display_factor: float | None = None
    """
    The multiplicative factor needed to convert the signal values from the raw unit to the display
    unit.
    """

    def to_dict(self) -> dict[str, Any]:
        return {
            'name'          : self.name,
            'index'         : self.index,
            'seriesRange'   : list(self.signal_range),
            'type'          : self.type,
            'coilType'      : self.coil_type,
            'unit'          : self.display_unit,
            'displayUnit'   : self.display_unit,
            'displayFactor' : self.display_factor,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> 'ChannelMetadata':
        return ChannelMetadata(
            name           = data['name'],
            index          = data['index'],
            signal_range   = tuple(data['seriesRange']),
            type           = data.get('type'),
            coil_type      = data.get('coilType'),
            unit           = data.get('unit'),
            display_unit   = data.get('displayUnit'),
            display_factor = data.get('displayFactor')
        )
