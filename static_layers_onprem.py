import json
import yaml
import copy
import os
import argparse

import subprocess
from pcm import PCM, AWS, SCRIPT_DIR, s3_upload_directory, logger, STATIC_CONFIG_PARAMS


def generate_configs(json_data, template_config, cli_posting=None):
    """
    Generates a list of runconfig dictionaries based on a template and a list of
    frame metadata.
    """
    generated_configs = []

    s3_urls = []

    for item in json_data:
        # Create a deep copy to ensure we don't modify the original
        new_config = copy.deepcopy(template_config)
        config_node = new_config['runconfig']['groups']

        # --- 1. Map Geometry & Grid ---
        config_node['processing']['geo_grid']['epsg'] = item['epsg']

        config_node['processing']['geo_grid']['top_left']['x'] = item['mapTopLeftX']
        config_node['processing']['geo_grid']['top_left']['y'] = item['mapTopLeftY']

        config_node['processing']['geo_grid']['bottom_right']['x'] = item['mapBottomRightX']
        config_node['processing']['geo_grid']['bottom_right']['y'] = item['mapBottomRightY']

        # --- 2. Map Ephemeris (Time) ---
        config_node['processing']['ephemeris']['start_time'] = item['aux_coverage']['matched_segment']['start']
        config_node['processing']['ephemeris']['end_time'] = item['aux_coverage']['matched_segment']['end']

        # --- 3. Map Orbit & Frame ---
        config_node['geometry']['relative_orbit_number'] = item['track']
        config_node['geometry']['frame_number'] = item['frame']

        # --- 4. Map Ancillary Files (Filename Only) ---
        orbit_path = item['aux_coverage']['files']['orbit']['path']
        pointing_path = item['aux_coverage']['files']['pointing']['path']

        config_node['dynamic_ancillary_file_group']['orbit_xml_file'] = os.path.basename(orbit_path)
        config_node['dynamic_ancillary_file_group']['pointing_xml_file'] = os.path.basename(pointing_path)

        # --- 4b. Map aux_coverage.bounding_box to radar_grid.bounding_box ---
        if 'bounding_box' in item['aux_coverage']:
            processing = config_node['processing']
            if 'radar_grid' not in processing:
                processing['radar_grid'] = {}
            if 'bounding_box' not in processing['radar_grid']:
                processing['radar_grid']['bounding_box'] = {}
            bbox = processing['radar_grid']['bounding_box']
            for key, value in item['aux_coverage']['bounding_box'].items():
                bbox[key] = value

        # --- 5. Handle Posting ---
        # Logic: Check specific JSON item first, then fallback to CLI arg, then keep template default.
        # Item posting may be a single value (x and y) or a pair: [x, y], (x, y), or {"x": _, "y": _}.
        posting_val = item.get('posting', cli_posting)

        if posting_val is not None:
            if isinstance(posting_val, (list, tuple)) and len(posting_val) >= 2:
                post_x_val = float(posting_val[0])
                post_y_val = float(posting_val[1])
            elif isinstance(posting_val, dict) and 'x' in posting_val and 'y' in posting_val:
                post_x_val = float(posting_val['x'])
                post_y_val = float(posting_val['y'])
            else:
                post_x_val = post_y_val = float(posting_val)
            config_node['processing']['geo_grid']['posting']['x'] = post_x_val
            config_node['processing']['geo_grid']['posting']['y'] = post_y_val

        # Look up slant_range_spacing from STATIC_CONFIG_PARAMS using (x, y) posting.
        # Keys are ints scaled by 10 so e.g. (2.5, 5) -> (25, 50) for reliable lookup.
        post_x = config_node['processing']['geo_grid']['posting']['x']
        post_y = config_node['processing']['geo_grid']['posting']['y']
        posting_key = (int(round(float(post_x) * 10)), int(round(float(post_y) * 10)))
        if posting_key in STATIC_CONFIG_PARAMS:
            params_list = STATIC_CONFIG_PARAMS[posting_key]
            slant_range_spacing = params_list[0].get('slant_range_spacing')
            if 'radar_grid' not in config_node['processing']:
                config_node['processing']['radar_grid'] = {}
            if 'spacing' not in config_node['processing']['radar_grid']:
                config_node['processing']['radar_grid']['spacing'] = {}
            config_node['processing']['radar_grid']['spacing']['rg_spacing'] = slant_range_spacing
        else:
            logger.warning(
                "Posting (x, y)=(%s, %s) not found in STATIC_CONFIG_PARAMS for track=%s, frame=%s. "
                "Expected one of: %s. rg_spacing will not be set for this config.",
                post_x, post_y, item['track'], item['frame'], sorted(STATIC_CONFIG_PARAMS.keys()),
            )
            response = input("Continue anyway? [y/N]: ").strip().lower()
            if response not in ('y', 'yes'):
                raise SystemExit(1)

        generated_configs.append({
            'track': item['track'],
            'frame': item['frame'],
            'orbit_xml': orbit_path,
            'pointing_xml': pointing_path,
            'config': new_config,
        })
        s3_urls += [orbit_path, pointing_path]

    if not AWS.all_s3_urls_exist(s3_urls, 'Ancilliary Files'):
        raise ValueError('Not all Ancilliary S3 URLs exist, check output JSON again.')

    return generated_configs


def main() -> int:
    """Main function."""
    parser = argparse.ArgumentParser(description="Generate runconfigs from NISAR coverage results.")

    # Required positional argument for the JSON input
    parser.add_argument('json_file', help="Path to nisar_coverage_results.json")

    # Determine the default path relative to this script's location
    # This finds the directory this script lives in, then looks for templates/static.yml
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_template_path = os.path.join(script_dir, 'templates', 'static-80.yml')

    # Optional argument for the config template
    parser.add_argument('-c', '--config',
                        default=default_template_path,
                        help=f"Path to template YAML file. Defaults to: {default_template_path}")
    parser.add_argument(
        '-o'
        '--output-bucket',
        dest='output_bucket',
        action='store',
        required=False,
        type=str,
        help='S3 URL to where the RSLC results will be uploaded.',
    )

    parser.add_argument('--posting', type=float, help="Global posting value override (optional)")

    args = parser.parse_args()

    # --- Validation ---
    if not os.path.exists(args.config):
        parser.error(f"The configuration file could not be found at: {args.config}\n"
                     "Please ensure the file exists or provide a path via --config.")

    # Load inputs
    with open(args.json_file, 'r') as f:
        coverage_data = json.load(f)
        if not isinstance(coverage_data, list):
            coverage_data = [coverage_data]

    with open(args.config, 'r') as f:
        yaml_template = yaml.safe_load(f)

    # Generate the list in memory
    config_list = generate_configs(coverage_data, yaml_template, args.posting)

    print(f"Successfully generated {len(config_list)} configurations in memory using template: {args.config}\n")

    outdir_list = []
    pcm = PCM()
    start_time_str, _ = pcm.get_str_time()

    # Submit jobs
    for config_args in config_list:
        outdir_list.append([(config_args['track'], config_args['frame']), pcm.run_static_layers_onprem(**config_args)])

    # Write manifest
    manifest_log = os.path.join(os.getcwd(), 'log', f'{os.path.basename(__file__).split(".")[0]}_{start_time_str}.log')
    PCM.write_manifest(manifest_log, outdir_list, header='track,frame,Static Layer Directory')
    logger.info(f'Manifest log written to: {manifest_log}')

    pcm.wait_for_completion()

    if args.output_bucket is None:
        # logger.warning(f'No output bucket specified, manifest log written to: {manifest_log}')
        logger.warning(f'No output bucket specified, now exiting...')
        return 0
    elif args.output_bucket[-1] == '/':
        args.output_bucket = args.output_bucket[:-1]

    # Copy results to the specified output bucket
    s3_upload_directory(outdir_list, args.output_bucket)

    return 0



if __name__ == "__main__":
    main()
