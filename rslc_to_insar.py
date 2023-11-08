import argparse
import os

from pcm import PCM, DEFAULT_BUCKET, SCRIPT_DIR, logger


def main() -> int:
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Submits a PCM job for converting RSLC data to GUNW.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        'rslc',
        type=str,
        help='S3 URLs to the first RSLCs to be processed.'
    )
    parser.add_argument(
        'rslc_n',
        type=str,
        nargs='+',
        help=argparse.SUPPRESS
    )
    parser.add_argument(
        '-o'
        '--output-bucket',
        dest='output_bucket',
        action='store',
        required=False,
        type=str,
        help='S3 URL to where the RSLC results will be uploaded.',
        default=f'{DEFAULT_BUCKET}/GUNW'
    )
    parser.add_argument(
        '-c',
        '--config',
        dest='config',
        action='store',
        required=False,
        type=str,
        help='Run config file for running insar.py.',
    )
    parser.add_argument(
        '-d',
        '--dem',
        dest='dem',
        action='store',
        required=False,
        type=str,
        help='Dem file for running insar.py',
    )
    args = parser.parse_args()
    
    # Sanity checking inputs
    if args.dem is None:
        logger.warning(f'No DEM file was specified, running stage_dem.py is suggested.')
        args.dem = ''
    
    if args.config is None:
        args.config = os.path.join(SCRIPT_DIR, 'templates', 'insar.yaml')
        logger.warning(f'No run config was specified, using default: {args.config}')
    
    if args.output_bucket is None:
        logger.warning(f'No output bucket specified, using default: {args.output_bucket}')
    
    with open(args.config, 'r', encoding='utf-8') as f:
        config = f.read()
    pcm = PCM()
    for i in range(len(args.rslc_n)):
        prev_rslc = args.rslc if i == 0 else args.rslc_n[i - 1]
        for next_rslc in args.rslc_n[i:]:
            pcm.run_rslc_to_insar(prev_rslc, next_rslc, args.dem, args.output_bucket, config=config)
    
    pcm.wait_for_completion()
    
    return 0

if __name__ == '__main__':
    main()
