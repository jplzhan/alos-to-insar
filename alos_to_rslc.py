import argparse
import os

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
    parser.add_argument(
        '-c',
        '--config',
        dest='config',
        action='store',
        required=False,
        type=str,
        help='Run config file for running focus.py.',
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
    
    if args.config is None:
        args.config = os.path.join(SCRIPT_DIR, 'templates', 'focus.yaml')
        logger.warning(f'No run config was specified, using default: {args.config}')
    
    if args.output_bucket is None:
        logger.warning(f'No output bucket specified, using default: {args.output_bucket}')
    
    with open(args.config, 'r', encoding='utf-8') as f:
        config = f.read()
    pcm = PCM()
    for link in args.data_links:
        pcm.run_alos_to_rslc(link, args.output_bucket, config=config)
    
    pcm.wait_for_completion()
    
    return 0

if __name__ == '__main__':
    main()
