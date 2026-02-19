from dataclasses import dataclass
from pathlib import Path


@dataclass
class BidsImportInfo:
    """
    Information about a specific BIDS import pipeline run.
    """

    source_bids_path: Path
    """
    The source BIDS directory path.
    """

    loris_bids_path: Path | None
    """
    The LORIS BIDS directory path for this import.
    """

    data_dir_path: Path
    """
    The LORIS data directory path.
    """
