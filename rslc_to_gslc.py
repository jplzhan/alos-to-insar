import argparse
import os
import subprocess

from pcm import PCM, DEFAULT_BUCKET, SCRIPT_DIR, logger


def main() -> int:
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Submits a PCM job for converting RSLC to GSLC.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        'data_links',
        type=str,
        nargs='+',
        help='S3 URL to the L0B data to convert to RSLC data.'
    )
    parser.add_argument(
        '-o'
        '--output-bucket',
        dest='output_bucket',
        action='store',
        required=False,
        type=str,
        help='S3 URL to where the RSLC results will be uploaded.',
        default=f'{DEFAULT_BUCKET}/GSLC'
    )
    parser.add_argument(
        '-c',
        '--config',
        dest='config',
        action='store',
        required=False,
        type=str,
        help='Run config file for running gslc.py.',
    )
    parser.add_argument(
        '-d',
        '--dem',
        dest='dem',
        action='store',
        required=False,
        type=str,
        help='Dem file for running gslc.py'
    )
    args = parser.parse_args()
    
    # Sanity checking inputs
    if args.dem is None:
        logger.warning(f'No DEM file was specified.')
        args.dem = ''
    
    if args.config is None:
        args.config = os.path.join(SCRIPT_DIR, 'templates', 'gslc.yaml')
        logger.warning(f'No run config was specified, using default: {args.config}')
    
    if args.output_bucket is None:
        logger.warning(f'No output bucket specified, using default: {args.output_bucket}')
    
    with open(args.config, 'r', encoding='utf-8') as f:
        config = f.read()
    outdir_list = []
    pcm = PCM()
    for link in args.data_links:
        outdir_list.append([link, pcm.run_rslc_to_gslc(link, args.dem, args.output_bucket, config=config)])

    pcm.wait_for_completion()

    # Copy results to the specified output bucket
    for link, outdir in outdir_list:
        logger.info(f'Copying results for {link} -> {outdir} -> {args.output_bucket}')
        subprocess.run(f'aws s3 cp --recursive {outdir}/ {args.output_bucket}/ --exclude "*" --include "*.h5"', shell=True, check=True)
    
    return 0

if __name__ == '__main__':
    main()
