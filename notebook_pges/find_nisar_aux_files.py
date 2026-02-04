import boto3
import os
from datetime import datetime, timedelta
from typing import Optional, TypedDict, Any


AUX_BUCKET = "nisar-ops-lts-fwd"


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

def find_nisar_aux_files(
    start_dt: datetime, 
    end_dt: datetime, 
    lookback_days: int = 3,
    s3_client: Optional[Any] = None
) -> SearchResult:
    """
    Finds S3 orbit (POE) and pointing (NRP) files covering the start_dt -> end_dt.
    
    Args:
        start_dt: Request start time.
        end_dt: Request end time.
        lookback_days: Days to search backwards.
        s3_client: Optional pre-initialized boto3 s3 client.
    """
    # Use provided client or initialize a new one (lazy initialization)
    if s3_client is None:
        s3 = boto3.client('s3')
    else:
        s3 = s3_client

    def parse_ts(ts_str: str) -> datetime:
        return datetime.strptime(ts_str, "%Y%m%dT%H%M%S")

    def find_file(product_type: str) -> Optional[FileResult]:
        for i in range(lookback_days + 1):
            search_date = start_dt - timedelta(days=i)
            prefix_date = search_date.strftime('%Y/%m/%d/')
            prefix = f"products/{product_type}/{prefix_date}"
            
            # Allow S3 errors to propagate
            response = s3.list_objects_v2(Bucket=AUX_BUCKET, Prefix=prefix)
            
            for obj in response.get('Contents', []):
                key = obj['Key']
                if not key.endswith('.xml'):
                    continue

                filename = os.path.basename(key)
                name_parts = os.path.splitext(filename)[0].split('_')
                
                file_end = parse_ts(name_parts[-1])
                file_start = parse_ts(name_parts[-2])

                if file_start <= start_dt and file_end >= end_dt:
                    return {
                        "path": f"s3://{AUX_BUCKET}/{key}",
                        "file_start": file_start,
                        "file_end": file_end,
                        "start_margin": start_dt - file_start,
                        "end_margin": file_end - end_dt
                    }
        return None

    return {
        "orbit": find_file("POE"),
        "pointing": find_file("NRP")
    }