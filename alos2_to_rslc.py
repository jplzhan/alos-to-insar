import argparse
import os
import subprocess

from pcm import PCM, DEFAULT_BUCKET, SCRIPT_DIR, logger


def main() -> int:
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Submits a PCM job for converting ALOS-1 data to RSLC.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        'data_links',
        type=str,
        nargs='+',
        help='S3 URL to the ALOS-1 data to convert to RSLC data.'
    )
    parser.add_argument(
        '-o'
        '--output-bucket',
        dest='output_bucket',
        action='store',
        required=False,
        type=str,
        help='S3 URL to where the RSLC results will be uploaded.',
        default=f'{DEFAULT_BUCKET}/RSLC'
    )
    # parser.add_argument(
    #     '-d',
    #     '--dem',
    #     dest='dem',
    #     action='store',
    #     required=False,
    #     type=str,
    #     help='Dem file for running focus.py'
    # )
    args = parser.parse_args()
    
    # Sanity checking inputs
    # if args.dem is None:
    #     logger.warning(f'No DEM file was specified.')
    #     args.dem = ''
    
    if args.output_bucket is None:
        logger.warning(f'No output bucket specified, using default: {args.output_bucket}')
    elif args.output_bucket[-1] == '/':
        args.output_bucket = args.output_bucket[:-1]
    
    outdir_list = []
    pcm = PCM()
    for link in args.data_links:
        outdir_list.append([link, pcm.run_alos2_to_rslc(link, args.output_bucket)])
    
    pcm.wait_for_completion()
    
    # Copy results to the specified output bucket
    for link, outdir in outdir_list:
        logger.info(f'Copying results for {link} -> {outdir} -> {args.output_bucket}')
        subprocess.run(f'aws s3 cp --recursive {outdir}/ {args.output_bucket}/ --exclude "*" --include "*.h5"', shell=True, check=True)
    
    return 0

if __name__ == '__main__':
    main()
