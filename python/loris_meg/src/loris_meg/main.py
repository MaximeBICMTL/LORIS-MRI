import math
import os
import re
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import mne
import mne_bids
import numpy as np
from fastapi import FastAPI, HTTPException
from lib.config import get_data_dir_path_config
from lib.config_file import load_config
from lib.db.queries.physio_file import try_get_physio_file_with_id
from lib.make_env import make_env

app = FastAPI()

# Load config
config = load_config("config.py")

# Create database engine
db_config = config.mysql


def get_bids_path(path: Path) -> mne_bids.BIDSPath:
    return mne_bids.BIDSPath(
        subject=re.search(r'sub-([0-9]+)', path.parent.parent.parent.name).group(1),
        session=re.search(r'sub-([0-9]+)', path.parent.parent.name).group(1),
        task=re.search(r'task-([a-zA-Z0-9]+)', path.name).group(1) if 'task-' in path.name else None,
        run=re.search(r'run-([0-9]+)', path.name).group(1) if 'run-' in path.name else None,
        datatype=path.parent.name,
        root=path.parent.parent.parent.parent,
        suffix=re.search(r'_([a-zA-Z0-9]+)\.', path.name).group(1) if re.search(r'_([a-zA-Z0-9]+)\.', path.name) else None,
        extension='' + '.'.join(path.suffixes),
    )


def as_json_object(value):
    """
    Recursively convert a value to a JSON-compatible type.
    """

    if value is None or isinstance(value, (str, int, bool)):
        return value

    # Handle float special cases
    if isinstance(value, float):
        if math.isinf(value) or math.isnan(value):
            return str(value)  # Convert inf/nan to string
        return value

    # Handle numpy types
    if isinstance(value, np.ndarray):
        # Handle array with special float values
        if value.dtype.kind == 'f':  # float array
            # Check for inf/nan
            if np.any(np.isinf(value)) or np.any(np.isnan(value)):
                return [str(x) if (isinstance(x, float) and (math.isinf(x) or math.isnan(x))) else as_json_object(x)
                       for x in value.tolist()]
        return value.tolist()

    if isinstance(value, np.integer):
        return int(value)

    if isinstance(value, np.floating):
        if np.isinf(value) or np.isnan(value):
            return str(value)
        return float(value)

    if isinstance(value, np.bool_):
        return bool(value)

    # Handle datetime/dates
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    # Handle Decimal
    if isinstance(value, Decimal):
        return float(value)

    # Handle UUID
    if isinstance(value, uuid.UUID):
        return str(value)

    # Handle iterables (list, tuple, set)
    if isinstance(value, (list, tuple, set)):
        return [as_json_object(item) for item in value]

    # Handle dictionaries
    if isinstance(value, dict):
        return {str(k): as_json_object(v) for k, v in value.items()}

    raise Exception(value)


@app.get("/meg/{physio_file_id}/channels")
def get_meg_channels(physio_file_id: int):
    env = make_env("server", {}, config, os.environ["TMPDIR"], False)

    # Fetch the physio file
    physio_file = try_get_physio_file_with_id(env.db, physio_file_id)
    if not physio_file:
        raise HTTPException(status_code=404, detail="Physiological file not found")

    data_dir_path = get_data_dir_path_config(env)

    match physio_file.type:
        case 'ctf':
            raw = mne.io.read_raw_ctf(data_dir_path / physio_file.path)
        case 'edf':
            raw = mne.io.read_raw_edf(data_dir_path / physio_file.path)
        case 'set':
            raw = mne.io.read_raw_eeglab(data_dir_path / physio_file.path)
        case str():
            raise HTTPException(status_code=404, detail=f"Unknown file type '{physio_file.type}'")
        case None:
            raise HTTPException(status_code=404, detail="No file type")

    channels = []
    for channel in raw.info["chs"]:
        channels.append(as_json_object(channel))

    return {"channels": channels}
