import geopandas as gpd
import numpy as np
import json
import pandas as pd
import logging
import boto3
from datetime import datetime, timedelta
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
worker_s3_client = None

def init_worker():
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
    def default(self, obj):
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        if isinstance(obj, timedelta):
            return str(obj)
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        return super().default(obj)

def process_single_frame(row_data: dict, cycle_times: np.ndarray, current_time: datetime) -> dict:
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
    
    # Iterate segments
    for seg_start, seg_end in segments:
        # Pass the global client initialized in init_worker
        result = find_nisar_aux_files(seg_start, seg_end, s3_client=worker_s3_client)
        
        if result['orbit'] is not None and result['pointing'] is not None:
            search_result = {
                "matched_segment": {
                    "start": seg_start,
                    "end": seg_end
                },
                "files": result
            }
            break 

    row_data['aux_coverage'] = search_result
    
    if search_result is not None:
        logger.debug(f"Row Match Found: {search_result['matched_segment']['start']}")
    else:
        logger.warning("Row: No coverage found.")

    return row_data

def main():
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
        
        # Gather results as they complete
        for future in futures:
            final_results.append(future.result())

    # 4. Save
    logger.info(f"Saving results to {OUTPUT_FILENAME}...")
    with open(OUTPUT_FILENAME, 'w') as f:
        json.dump(final_results, f, cls=NisarEncoder, indent=4)
    
    logger.info("Done.")

if __name__ == "__main__":
    main()
