import argparse
import os
import subprocess

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
        help='S3 URLs to the RSLCs to be processed. If only 1 input is provided, then the input should be a .csv file containing the pairs to be processed on each line.'
    )
    parser.add_argument(
        'rslc_n',
        type=str,
        nargs='*',
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
    outdir_list = []
    pcm = PCM()
    
    if len(args.rslc_n) > 0:
        for i in range(len(args.rslc_n)):
            prev_rslc = args.rslc if i == 0 else args.rslc_n[i - 1]
            for next_rslc in args.rslc_n[i:]:
                # Sanity checker
                if prev_rslc == next_rslc:
                    logger.error(f'WARNING: Detected pair using "{prev_rslc}" as both the reference and secondary RSLC, which will not generate a useful product. Aborting submission script...\n')
                    return 1
                outdir_list.append([prev_rslc, next_rslc, pcm.run_rslc_to_insar(prev_rslc, next_rslc, args.dem, args.output_bucket, config=config)])
    else:
        with open(args.rslc, 'r', encoding='utf-8') as f:
            for line in f:
                urls = line.split(',')
                # Sanity checker
                if urls[0] == urls[1]:
                    logger.error(f'WARNING: Detected pair using {urls[0]} as both the reference and secondary RSLC, which will not generate a useful product. Aborting submission script..\n')
                    return 1
                outdir_list.append([urls[0], urls[1], pcm.run_rslc_to_insar(urls[0], urls[1], args.dem, args.output_bucket, config=config)])
    
    pcm.wait_for_completion()
    
    # Copy results to the specified output bucket
    for link_1, link_2, outdir in outdir_list:
        logger.info(f'Copying results for {link_1} and {link_2} -> {outdir} -> {args.output_bucket}')
        subprocess.run(f'aws s3 cp --recursive {outdir}/ {args.output_bucket}/ --exclude "*" --include "*.h5"', shell=True, check=True)
    
    return 0

if __name__ == '__main__':
    main()
