import argparse
import os
import subprocess

from pcm import PCM, AWS, SCRIPT_DIR, logger


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
    )
    args = parser.parse_args()

    outdir_list = []
    pcm = PCM()
    start_time_str, _ = pcm.get_str_time()

    # Pre-sanitize ALOS-2 data links
    module_name = 'ALOS-2'
    if not AWS.all_s3_urls_exist(args.data_links, module_name):
        return 1
    # Submit jobs
    for link in args.data_links:
        outdir_list.append([link, pcm.run_alos2_to_rslc(link)])
    # Write manifest
    manifest_log = os.path.join(os.getcwd(), 'log', f'{os.path.basename(__file__).split(".")[0]}_{start_time_str}.log')
    PCM.write_manifest(manifest_log, outdir_list, header='ALOS-2 Data,RSLC Directory')
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
