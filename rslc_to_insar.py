import argparse
import os
import subprocess

from pcm import PCM, AWS, SCRIPT_DIR, logger


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
        help='DEM file for running insar.py',
    )
    args = parser.parse_args()
    
    # Sanity checking inputs
    if args.dem is None:
        logger.warning(f'No DEM file was specified, running stage_dem.py is suggested.')
        args.dem = ''
    elif not AWS.s3_url_exists(args.dem):
        logger.error(f'DEM: {args.dem} does not exist. Aborting...')
        return 1
    
    if args.config is None:
        args.config = os.path.join(SCRIPT_DIR, 'templates', 'insar.yaml')
        logger.warning(f'No run config was specified, using default: {args.config}')

    with open(args.config, 'r', encoding='utf-8') as f:
        config = f.read()
    outdir_list = []
    pcm = PCM()
    start_time_str, _ = pcm.get_str_time()

    module_name = 'RSLC'
    if len(args.rslc_n) > 0:
        # Process pairs based on the command line if there is more than 1 positional argument
        # Dry run to sanity check the inputs
        for i in range(len(args.rslc_n)):
            prev_rslc = args.rslc if i == 0 else args.rslc_n[i - 1]
            for next_rslc in args.rslc_n[i:]:
                if prev_rslc == next_rslc:
                    logger.error(f'WARNING: Detected pair using "{prev_rslc}" as both the reference and secondary RSLC.')
                    logger.error('This will not generate a useful product. Aborting submission script..\n')
                    return 1
                if not AWS.all_s3_urls_exist([prev_rslc, next_rslc], module_name):
                    return 1
        # Submit the jobs
        for i in range(len(args.rslc_n)):
            prev_rslc = args.rslc if i == 0 else args.rslc_n[i - 1]
            for next_rslc in args.rslc_n[i:]:
                outdir_list.append([prev_rslc, next_rslc, pcm.run_rslc_to_insar(prev_rslc, next_rslc, args.dem, config=config)])
    else:
        # Process pairs based on an input CSV file if there is only 1 positional argument
        with open(args.rslc, 'r', encoding='utf-8') as f:
            csv_table = [row.split(',') for row in f.readlines()]
            # Dry run to sanity check the inputs
            for urls in csv_table:
                if urls[0] == urls[1]:
                    logger.error(f'WARNING: Detected pair using {urls[0]} as both the reference and secondary RSLC.')
                    logger.error('This will not generate a useful product. Aborting submission script..\n')
                    return 1
                if not AWS.all_s3_urls_exist([urls[0], urls[1]], module_name):
                    return 1
            # Submit the jobs
            for urls in csv_table:
                outdir_list.append([urls[0], urls[1], pcm.run_rslc_to_insar(urls[0], urls[1], args.dem, config=config)])
    # Write manifest
    manifest_log = os.path.join(os.getcwd(), 'log', f'{os.path.basename(__file__).split(".")[0]}_{start_time_str}.log')
    PCM.write_manifest(manifest_log, outdir_list, header='RSLC Data,INSAR Directory')
    logger.info(f'Manifest log written to: {manifest_log}')

    pcm.wait_for_completion()
    
    if args.output_bucket is None:
        logger.warning(f'No output bucket specified, manifest log written to: {manifest_log}')
        return 0
    elif args.output_bucket[-1] == '/':
        args.output_bucket = args.output_bucket[:-1]

    # Copy results to the specified output bucket
    for link_1, link_2, outdir in outdir_list:
        logger.info(f'Copying results for {link_1} and {link_2} -> {outdir} -> {args.output_bucket}')
        subprocess.run(f'aws s3 cp --recursive {outdir}/ {args.output_bucket}/ --exclude "*" --include "*.h5"', shell=True, check=True)
    
    return 0

if __name__ == '__main__':
    main()
