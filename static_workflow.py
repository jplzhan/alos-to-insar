import argparse
import os
import subprocess

from pcm import PCM, AWS, SCRIPT_DIR, logger


def main() -> int:
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Submits a PCM job for running the static layers workflow.',
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
    parser.add_argument(
        '-c',
        '--config',
        dest='config',
        action='store',
        required=False,
        type=str,
        help='Run config file for running static.py.',
    )
    parser.add_argument(
        '-b',
        '--orbit',
        dest='orbit',
        action='store',
        required=True,
        type=str,
        help='Orbit XML file for running static.py.',
    )
    parser.add_argument(
        '-p',
        '--pointing',
        dest='pointing',
        action='store',
        required=True,
        type=str,
        help='Pointing XML file for running static.py.',
    )
    parser.add_argument(
        '-d',
        '--dem',
        dest='dem',
        action='store',
        required=False,
        type=str,
        help='DEM VRT file for running static.py'
    )
        parser.add_argument(
        '-w',
        '--watermask',
        dest='watermask',
        action='store',
        required=False,
        type=str,
        help='Watermask VRT file for running static.py'
    )
    args = parser.parse_args()
    
    # Sanity checking inputs
    if args.dem is None:
        logger.warning(f'No DEM VRT file was specified.')
        args.dem = ''
    elif not AWS.s3_url_exists(args.dem):
        logger.error(f'DEM VRT: {args.dem} does not exist. Aborting...')
        return 1

    if args.watermask is None:
        logger.warning(f'No Watermask VRT file was specified.')
        args.watermask = ''
    elif not AWS.s3_url_exists(args.watermask):
        logger.error(f'WATERMASK VRT: {args.watermask} does not exist. Aborting...')
        return 1
    
    if args.config is None:
        args.config = os.path.join(SCRIPT_DIR, 'templates', 'static.yaml')
        logger.warning(f'No run config was specified, using default: {args.config}')

    with open(args.config, 'r', encoding='utf-8') as fp:
        config = fp.read()

    with open(args.orbit, 'r', encoding='utf-8') as fp:
        orbit_xml = fp.read()

    with open(args.pointing, 'r', encoding='utf-8') as fp:
        pointing_xml = fp.read()
        
    outdir_list = []
    pcm = PCM()
    start_time_str, _ = pcm.get_str_time()

    # Submit jobs
    outdir_list.append(['Empty', pcm.run_static_workflow(orbit_xml=orbit_xml, pointing_xml=pointing_xml,
                                                         dem=args.dem, watermask=args.watermask, config=config)])

    # Write manifest
    # manifest_log = os.path.join(os.getcwd(), 'log', f'{os.path.basename(__file__).split(".")[0]}_{start_time_str}.log')
    # PCM.write_manifest(manifest_log, outdir_list, header='L0B Data,Static Layer Directory')
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
