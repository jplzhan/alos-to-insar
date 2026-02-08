import boto3
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, TypedDict, Any, Callable

logger = logging.getLogger(__name__)

AUX_BUCKET = "nisar-ops-lts-fwd"
RSLC_BUCKET = "nisar-ops-rs-fwd"


# Define the return structure for a single file result
class FileResult(TypedDict):
    path: str
    file_start: datetime
    file_end: datetime
    start_margin: timedelta
    end_margin: timedelta

# Define the overall return structure
class SearchResult(TypedDict):
    orbit: Optional[FileResult]
    pointing: Optional[FileResult]
    rslc: Optional[FileResult]

def find_nisar_aux_files(
    start_dt: datetime,
    end_dt: datetime,
    lookback_days: int = 3,
    s3_client: Optional[Any] = None,
    track: Optional[int] = None,
    frame: Optional[int] = None,
) -> SearchResult:
    """
    Finds S3 orbit (POE), pointing (NRP), and RSLC files.
    Orbit and pointing are matched by time; RSLC is matched by track/frame from filename
    (e.g. RSLC_012_028_D_060 -> track 28, frame 60), still searched by date.

    Args:
        start_dt: Request start time.
        end_dt: Request end time.
        lookback_days: Days to search backwards.
        s3_client: Optional pre-initialized boto3 s3 client.
        track: Track number for RSLC filename match (e.g. 28 from RSLC_xxx_028_*).
        frame: Frame number for RSLC filename match (e.g. 60 from RSLC_xxx_*_060).
    """
    # Use provided client or initialize a new one (lazy initialization)
    if s3_client is None:
        s3 = boto3.client('s3')
    else:
        s3 = s3_client

    def parse_ts(ts_str: str) -> datetime:
        return datetime.strptime(ts_str, "%Y%m%dT%H%M%S")

    def timestamps_last_two(parts: list[str]) -> Optional[tuple[datetime, datetime]]:
        try:
            return (parse_ts(parts[-2]), parse_ts(parts[-1]))
        except (ValueError, IndexError):
            return None

    def timestamps_parseable(parts: list[str]) -> Optional[tuple[datetime, datetime]]:
        timestamps = []
        for part in parts:
            try:
                timestamps.append(parse_ts(part))
            except ValueError:
                pass
        return (timestamps[-2], timestamps[-1]) if len(timestamps) >= 2 else None

    def find_file(
        bucket: str,
        prefix_dir: str,
        suffix: str,
        get_timestamps: Callable[[list[str]], Optional[tuple[datetime, datetime]]],
        *,
        match_within_seconds: Optional[int] = None,
    ) -> Optional[FileResult]:
        for i in range(lookback_days + 1):
            search_date = start_dt - timedelta(days=i)
            prefix_date = search_date.strftime('%Y/%m/%d/')
            prefix = f"{prefix_dir}{prefix_date}"
            logger.debug("Searching s3://%s/%s", bucket, prefix)
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            for obj in response.get('Contents', []):
                key = obj['Key']
                if not key.endswith(suffix):
                    continue
                name_parts = os.path.splitext(os.path.basename(key))[0].split('_')
                ts = get_timestamps(name_parts)
                if ts is None:
                    continue
                file_start, file_end = ts
                contained = file_start <= start_dt and file_end >= end_dt
                within_tol = (
                    match_within_seconds is not None
                    and abs((file_start - start_dt).total_seconds()) <= match_within_seconds
                    and abs((file_end - end_dt).total_seconds()) <= match_within_seconds
                )
                if contained or within_tol:
                    logger.debug("Match found s3://%s/%s", bucket, key)
                    return {
                        "path": f"s3://{bucket}/{key}",
                        "file_start": file_start,
                        "file_end": file_end,
                        "start_margin": start_dt - file_start,
                        "end_margin": file_end - end_dt
                    }
        return None

    def find_rslc_by_track_frame() -> Optional[FileResult]:
        """Match RSLC by track/frame in filename (RSLC_cycle_track_A|D_frame...)."""
        if track is None or frame is None:
            return None
        for i in range(lookback_days + 1):
            search_date = start_dt - timedelta(days=i)
            prefix_date = search_date.strftime('%Y/%m/%d/')
            prefix = f"products/L1_L_RSLC/{prefix_date}"
            logger.debug("Searching s3://%s/%s", RSLC_BUCKET, prefix)
            response = s3.list_objects_v2(Bucket=RSLC_BUCKET, Prefix=prefix)
            for obj in response.get('Contents', []):
                key = obj['Key']
                if not key.endswith('.h5'):
                    continue
                name_parts = os.path.splitext(os.path.basename(key))[0].split('_')
                # RSLC_012_028_D_060 -> track=028 (idx 5), frame=060 (idx 7); letter at 6 is A or D
                if len(name_parts) < 8 or name_parts[3] != 'RSLC':
                    continue
                try:
                    file_track = int(name_parts[5])
                    file_frame = int(name_parts[7])
                except (ValueError, IndexError):
                    continue
                if file_track != track or file_frame != frame:
                    continue
                ts = timestamps_parseable(name_parts)
                if ts is None:
                    continue
                file_start, file_end = ts
                logger.debug("Match found s3://%s/%s", RSLC_BUCKET, key)
                return {
                    "path": f"s3://{RSLC_BUCKET}/{key}",
                    "file_start": file_start,
                    "file_end": file_end,
                    "start_margin": start_dt - file_start,
                    "end_margin": file_end - end_dt
                }
        return None

    return {
        "orbit": find_file(AUX_BUCKET, "products/POE/", ".xml", timestamps_last_two),
        "pointing": find_file(AUX_BUCKET, "products/NRP/", ".xml", timestamps_last_two),
        "rslc": find_rslc_by_track_frame(),
    }