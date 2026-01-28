import argparse
import os
import subprocess

from pcm import PCM, AWS, SCRIPT_DIR, logger


def main() -> int:
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Submits a PCM job for running the test local DEM workflow.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '-o'
        '--output-bucket',
        dest='output_bucket',
        action='store',
        required=False,
        type=str,
        help='S3 URL to where the static layer results will be uploaded.',
    )
    args = parser.parse_args()
        
    outdir_list = []
    pcm = PCM()
    start_time_str, _ = pcm.get_str_time()

    # Submit jobs
    outdir_list.append(['Empty', pcm.run_test_local_dem(track=1,frame=5)])

    pcm.wait_for_completion()

    return 0

if __name__ == '__main__':
    main()
