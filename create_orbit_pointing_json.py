import argparse
import geopandas as gpd
import numpy as np
import json
import pandas as pd
import logging
import boto3
import h5py
from datetime import datetime, timedelta
from typing import Any
from shapely.geometry import mapping
from concurrent.futures import ProcessPoolExecutor
from notebook_pges.find_nisar_aux_files import find_nisar_aux_files

# --- Configuration ---
GPKG_FILENAME = "NISAR_TrackFrame_L_20250909.gpkg"
CYCLE_TIMES_FILENAME = "nisar_cycle_times.npy"
OUTPUT_FILENAME = "nisar_coverage_results.json"
MAX_WORKERS = None  # None defaults to number of processors on the machine

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Global var for Worker Process ---
worker_s3_client: Any = None


def get_master_creds_dict() -> dict[str, Any]:
    """
    Returns a dictionary of STATIC credentials to pass to the h5py ROS3 driver.
    Forces a refresh to ensure the tokens have the maximum possible TTL.
    """
    session = boto3.Session()
    creds = session.get_credentials()

    if not creds:
        raise RuntimeError("Could not locate AWS credentials. Check env vars or ~/.aws/credentials.")

    if hasattr(creds, 'refresh'):
        try:
            creds.refresh()
        except Exception as e:
            logger.warning("Could not refresh credentials: %s. Using current values.", e)

    frozen_creds = creds.get_frozen_credentials()
    if not frozen_creds:
        raise RuntimeError("Failed to retrieve frozen credentials from boto3.")

    region = session.region_name or 'us-west-2'
    return {
        'driver': 'ros3',
        'page_buf_size': 512,
        'aws_region': region.encode('utf-8'),
        'secret_id': frozen_creds.access_key.encode('utf-8'),
        'secret_key': frozen_creds.secret_key.encode('utf-8'),
        'session_token': (frozen_creds.token or '').encode('utf-8'),
    }


def _h5_read_timestamp(ds: Any) -> str:
    """Read a timestamp dataset (string or bytes) from HDF5 as a string."""
    val = ds[()]
    if isinstance(val, bytes):
        return val.decode("utf-8")
    return str(val)


def read_rslc_bounding_box(rslc_s3_uri: str) -> dict[str, float | str] | None:
    """Read start/end range and zero-Doppler times from RSLC HDF5; return dict or None on error."""
    try:
        h5_creds = get_master_creds_dict()
        with h5py.File(rslc_s3_uri, 'r', **h5_creds) as h5:
            # h5py Group.__getitem__(name): "name may be a relative or absolute path" (docs)
            slant = np.asarray(h5["/science/LSAR/RSLC/swaths/frequencyA/slantRange"])
            start_range = float(slant.flat[0])
            end_range = float(slant.flat[-1])
            start_time = _h5_read_timestamp(h5["/science/LSAR/identification/zeroDopplerStartTime"])
            end_time = _h5_read_timestamp(h5["/science/LSAR/identification/zeroDopplerEndTime"])
        return {
            "start_range": start_range,
            "end_range": end_range,
            "start_time": start_time,
            "end_time": end_time,
        }
    except Exception as e:
        logger.warning("Failed to read RSLC bounding box from %s: %s", rslc_s3_uri, e)
        return None


def init_worker() -> None:
    """
    Initializer for worker processes.
    Creates a single S3 client that persists for the life of the worker process.
    """
    global worker_s3_client
    # We create the client once per process
    worker_s3_client = boto3.client('s3')

def filter_database(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """User-defined filter logic."""
    return gdf[gdf['isCalVal'] == True]

class NisarEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        if isinstance(obj, timedelta):
            return str(obj)
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        return super().default(obj)

def process_single_frame(
    row_data: dict[str, Any],
    cycle_times: np.ndarray,
    current_time: datetime,
) -> dict[str, Any]:
    """
    Worker function to process a single frame.
    Finds the first valid time segment in the past that has coverage.
    """
    start_cy_sec = row_data.get('startCY')
    end_cy_sec = row_data.get('endCY')

    if start_cy_sec is None or end_cy_sec is None:
        row_data['aux_files'] = None
        return row_data

    # Generate segments
    segments = []
    for cycle_start in cycle_times:
        if isinstance(cycle_start, np.datetime64):
            cycle_start = cycle_start.astype(datetime)

        seg_start = cycle_start + timedelta(seconds=float(start_cy_sec))
        seg_end = cycle_start + timedelta(seconds=float(end_cy_sec))

        if seg_start <= current_time:
            segments.append((seg_start, seg_end))

    # Sort newest first
    segments.sort(key=lambda x: x[0], reverse=True)

    search_result = None

    # Track/frame for RSLC filename match (e.g. RSLC_012_028_D_060 -> track 28, frame 60)
    track = row_data.get("track")
    frame = row_data.get("frame")
    if track is not None:
        track = int(track)
    if frame is not None:
        frame = int(frame)

    # Iterate segments
    for seg_start, seg_end in segments:
        # Pass the global client initialized in init_worker
        result = find_nisar_aux_files(
            seg_start, seg_end,
            s3_client=worker_s3_client,
            track=track, frame=frame,
        )

        # result includes orbit, pointing, and rslc (RSLC from nisar-ops-rs-fwd)
        if all(result[k] is not None for k in ('orbit', 'pointing', 'rslc')):
            rslc_path = result['rslc']['path']
            bounding_box = read_rslc_bounding_box(rslc_path)
            search_result = {
                "matched_segment": {
                    "start": seg_start,
                    "end": seg_end
                },
                "files": result,
                "bounding_box": bounding_box
            }
            break

    row_data['aux_coverage'] = search_result

    if search_result is not None:
        logger.debug("Row Match Found: %s", search_result['matched_segment']['start'])
    else:
        # Include row identifiers and segment info to make the failure traceable
        ids = {k: row_data.get(k) for k in ('track', 'frame') if row_data.get(k) is not None}
        seg_range = f" {segments[0][0]} to {segments[-1][1]}" if segments else " (no segments)"
        logger.warning(
            "No coverage found for row: identifiers=%s, startCY=%s, endCY=%s, segments_checked=%d, segment_range=%s",
            ids, start_cy_sec, end_cy_sec, len(segments), seg_range,
        )

    return row_data

def main() -> None:
    # 1. Load Data (No Try-Except)
    logger.info(f"Loading {GPKG_FILENAME}...")
    gdf = gpd.read_file(GPKG_FILENAME)

    logger.info(f"Loading {CYCLE_TIMES_FILENAME}...")
    cycle_times = np.load(CYCLE_TIMES_FILENAME, allow_pickle=True)

    # 2. Filter
    target_rows = filter_database(gdf)
    logger.info(f"Processing {len(target_rows)} rows using multiprocessing.")

    current_time = datetime.now()

    # Prepare data for mapping
    # We convert rows to dicts upfront to avoid passing complex DataFrame slices to processes
    row_inputs = []
    for _, row in target_rows.iterrows():
        r_dict = row.to_dict()
        if 'geometry' in r_dict and r_dict['geometry']:
            r_dict['geometry'] = mapping(r_dict['geometry'])
        row_inputs.append(r_dict)

    final_results = []

    # 3. Multiprocessing Execution
    # uses init_worker to set up S3 client per core
    with ProcessPoolExecutor(max_workers=MAX_WORKERS, initializer=init_worker) as executor:
        # Submit all tasks
        futures = [
            executor.submit(process_single_frame, row, cycle_times, current_time)
            for row in row_inputs
        ]

        # Gather results as they complete; drop rows with no match
        for future in futures:
            row_result = future.result()
            if row_result.get("aux_coverage") is not None:
                final_results.append(row_result)

    # 4. Save
    logger.info(f"Saving results to {OUTPUT_FILENAME}...")
    with open(OUTPUT_FILENAME, 'w') as f:
        json.dump(final_results, f, cls=NisarEncoder, indent=4)

    logger.info("Done.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build NISAR orbit/pointing/RSLC coverage JSON from track-frame gpkg.")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging (e.g. S3 buckets and prefixes searched).",
    )
    args = parser.parse_args()
    if args.debug:
        # Only our code: avoid DEBUG from boto3, h5py, urllib3, etc.
        for name in ("__main__", "notebook_pges.find_nisar_aux_files"):
            logging.getLogger(name).setLevel(logging.DEBUG)
    main()
