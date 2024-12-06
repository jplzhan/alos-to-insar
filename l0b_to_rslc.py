import argparse
import os
import subprocess

from pcm import PCM, AWS, SCRIPT_DIR, logger


def main() -> int:
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Submits a PCM job for converting NISAR L0B data to RSLC.',
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
    # elif not AWS.s3_url_exists(args.dem):
    #     logger.error(f'DEM: {args.dem} does not exist. Aborting...')
    #     return 1
    
    if args.config is None:
        args.config = os.path.join(SCRIPT_DIR, 'templates', 'focus.yaml')
        logger.warning(f'No run config was specified, using default: {args.config}')

    with open(args.config, 'r', encoding='utf-8') as f:
        config = f.read()
    outdir_list = []
    pcm = PCM()
    start_time_str, _ = pcm.get_str_time()

    # Pre-sanitize L0B data links
    module_name = 'L0B'
    if not AWS.all_s3_urls_exist(args.data_links, module_name):
        return 1
    # Submit jobs
    for link in args.data_links:
        outdir_list.append([link, pcm.run_l0b_to_rslc(link, config=config)])
    # Write manifest
    manifest_log = os.path.join(os.getcwd(), 'log', f'{os.path.basename(__file__).split(".")[0]}_{start_time_str}.log')
    PCM.write_manifest(manifest_log, outdir_list, header='L0B Data,RSLC Directory')
    logger.info(f'Manifest log written to: {manifest_log}')

    pcm.wait_for_completion()

    if args.output_bucket is None:
        logger.warning(f'No output bucket specified, manifest log written to: {manifest_log}')
        return 0
    elif args.output_bucket[-1] == '/':
        args.output_bucket = args.output_bucket[:-1]

    # Copy results to the specified output bucket
    for link, outdir in outdir_list:
        logger.info(f'Copying results for {link} -> {outdir} -> {args.output_bucket}')
        subprocess.run(f'aws s3 cp --recursive {outdir}/ {args.output_bucket}/ --exclude "*" --include "*.h5"', shell=True, check=True)
    
    return 0

if __name__ == '__main__':
    main()
