"""PCM Job Submission API, which is a wrapper around Otello and Mozart APIs'."""
import argparse
import configparser
import logging
import boto3
import shutil
import argparse
import json
import os
import sys
import time
import yaml
from urllib.parse import urlparse
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from datetime import datetime, timezone, timedelta
from multiprocessing import Pool, cpu_count
from functools import partial

import otello

import notebook_pges.isce3_regex as isce3_regex
from notebook_pges.aws_uploader import AWS
import renameH5Product


# Default settings
DETECTED_ODS = 'adt' if AWS.is_accessible('nisar-adt', verbose=False) else 'st'
DEFAULT_BUCKET = {
    'adt': 's3://nisar-adt/ALOS-1-data',
    'st': 's3://nisar-st-data-ondemand/ALOS-1-data'
}[DETECTED_ODS]
DEFAULT_PCM_STORAGE = f's3://nisar-{DETECTED_ODS}-rs-ondemand/products'
DEFAULT_REPO = 'https://github.com/jplzhan/alos-to-insar.git'
DEFAULT_VERSION = 'v3.0.6'
DEFAULT_BUILD_TICK_SECONDS = 30
DEFAULT_AWS_PROFILE = 'saml-pub'
DEFAULT_POLARIZATION = 'HH'
NON_INSAR_POLARIZATION = f'{DEFAULT_POLARIZATION}00'


# The directory this script is being ran in
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


# Logger
log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(stream=sys.stdout, format=log_format, level=logging.INFO)
logger = logging.getLogger('pcm_logger')



# Posting keys are (x*10, y*10) as ints so e.g. (2.5, 5) -> (25, 50) for reliable lookup.
STATIC_CONFIG_PARAMS = {
    (800, 800): [
        {
            "l2_product": "GUNW, GOFF",
            "range_bandwidth": "All",
            "slant_range_spacing": None
        },
        {
            "l2_product": "GCOV",
            "range_bandwidth": "5 MHz",
            "slant_range_spacing": 24.98270483
        }
    ],
    (200, 200): [
        {
            "l2_product": "GCOV",
            "range_bandwidth": "20 MHz",
            "slant_range_spacing": 6.245676208
        },
        {
            "l2_product": "GCOV",
            "range_bandwidth": "77 MHz",
            "slant_range_spacing": 1.561419052
        }
    ],
    (100, 100): [
        {
            "l2_product": "GCOV",
            "range_bandwidth": "40 MHz",
            "slant_range_spacing": 3.122838104
        }
    ],
    (400, 50): [
        {
            "l2_product": "GSLC",
            "range_bandwidth": "5 MHz",
            "slant_range_spacing": 24.98270483
        }
    ],
    (100, 50): [
        {
            "l2_product": "GSLC",
            "range_bandwidth": "20 MHz",
            "slant_range_spacing": 6.245676208
        }
    ],
    (50, 50): [
        {
            "l2_product": "GSLC",
            "range_bandwidth": "40 MHz",
            "slant_range_spacing": 3.122838104
        }
    ],
    (25, 50): [
        {
            "l2_product": "GSLC",
            "range_bandwidth": "77 MHz",
            "slant_range_spacing": 1.561419052
        }
    ]
}



class PCM:
    """Class for launching PCM jobs."""
    def __init__(self,
                 repo: str=DEFAULT_REPO,
                 version: str=DEFAULT_VERSION,
                 rebuild: bool=False,
                 profile: str=DEFAULT_AWS_PROFILE):
        # Jenkins docker build
        self.repo = repo
        self.version = version
        self.ci = otello.CI(repo=repo, branch=version)
        self.build_pcm()
        self.mozart = otello.Mozart()
        self.job_set = otello.JobSet()
        self.num_jobs = 0

        # TODO - Check that all job types exist
        self.job_types = self.mozart.get_job_types()
        self.jobs = {
            'alos_to_rslc': None,
            'alos2_to_rslc': None,
            'l0b_to_rslc': None,
            'rslc_to_gslc': None,
            'rslc_to_gcov': None,
            'rslc_to_insar': None,
            'static_workflow': None,
            'static_layers_onprem': None,
        }

    def build_pcm(self, tick_rate:float=DEFAULT_BUILD_TICK_SECONDS, rebuild: bool=False):
        """Initialization method for building the PCM job.

        Input Parameters:
            - tick_rate: Number of seconds to wait before refreshing the build status.
            - rebuild: Whether or not to rebuild the job even if the previous job was a success.

        Raises a RuntimeError if the build is unsuccessful.
        """
        # Check if the job has previously been built
        try:
            self.ci.check_job_exists()
        except Exception as e:
            logger.info(f'Error ecnountered while checking for registration: {e}')
            logger.info(f'\'{self.repo}:{self.version}\' was unregistered. Now registering...')
            self.ci.register()

        # Check to see if the job was built, and if not, submit a build request
        try:
            build_status = self.ci.get_build_status()
            logger.info(f'Current build status: {build_status}')
            if build_status['result'] != 'SUCCESS' or rebuild:
                if not build_status['building']:
                    logger.info(f'Now rebuilding \'{self.repo}:{self.version}\' (estimated build time: ~30-45 minutes)...')
                    status = self.ci.submit_build()
                    logger.info(status)
                    time.sleep(10)
        except Exception as e:
            logger.debug(f'Exception caught while checking build status: {e}')
            logger.debug(f'Exceptions are thrown when the CI job was never built (other errors notwithstanding).')
            logger.info(f'Now submitting a build for \'{self.repo}:{self.version}\' (estimated build time: ~45 minutes)...')
            status = self.ci.submit_build()
            time.sleep(10)

        # Wait for the job to finish building
        build_status = self.ci.get_build_status()
        while build_status["building"]:
            logger.debug(build_status)
            logger.info(f'{self.repo}:{self.version} is still building, please wait...')
            time.sleep(tick_rate)
            build_status = self.ci.get_build_status()

        if build_status['result'] != 'SUCCESS':
            raise RuntimeError(f'{self.repo}:{self.version} failed, please check the build URL:\n{build_status}')

        logger.info(f'\'{self.repo}:{self.version}\' successfully built, now continuing...')

    def wait_for_completion(self, wait_seconds: float=15):
        """Wait for all submitted jobs in the job set to complete."""
        logger.info(f'Waiting {wait_seconds} seconds for start up...')
        time.sleep(wait_seconds)
        logger.info(f'Now waiting for completion of {len(self.job_set)} jobs...')
        self.job_set.wait_for_completion()

    def get_str_time(self) -> str:
        """Returns the current timestamp in the format used for PCM stage-out and folder format.

        The number of jobs is added in seconds to ensure that no two jobs will have the same timestamp.
        """
        now = datetime.now() + timedelta(0, self.num_jobs)
        return now.strftime('%Y%m%dT%H%M%S'), now.strftime('%Y/%m/%d')

    def run_alos_to_rslc(self,
                        data_link: str,
                        gpu_enabled: bool=True,
                        config: str='',
                        queue: str='nisar-job_worker-sciflo-rslc') -> str:
        """Runs ALOS to RSLC.

        Input Parameters:
            - data_link: S3 URL to the ALOS-1 data to be processed into NISAR L0B and then RSLC.
            - gpu_enabled: Whether to run focus.py using the GPU.
            - config: YAML formatted string containing the config to pass to focus.py.
            - queue: Name of the PCM queue to submit the job to (recommended is default).
        """
        ts, folder = self.get_str_time()
        jt = self.get_job('alos_to_rslc')
        jt.set_input_params({
            'data_link': data_link,
            'gpu_enabled': '1' if gpu_enabled else '0',
            'focus_config': str(config),
            'timestamp': ts,
        })
        ret = isce3_regex.RSLC_FORMAT.format(
            polarization=NON_INSAR_POLARIZATION,
            timestamp=ts)
        ret = f'{DEFAULT_PCM_STORAGE}/L1_L_RSLC/{folder}/{ret}'
        logger.info(f'Submitting ALOS to RSLC conversion job for {data_link}... (storage: {ret})')
        self.job_set.append(jt.submit_job(queue=queue))
        self.num_jobs += 1
        return ret

    def run_alos2_to_rslc(self,
                        data_link: str,
                        gpu_enabled: bool=True,
                        queue: str='nisar-job_worker-sciflo-rslc') -> str:
        """Runs ALOS-2 to RSLC.

        Input Parameters:
            - data_link: S3 URL to the ALOS-2 data to be processed into NISAR RSLC.
            - gpu_enabled: Whether to run focus.py using the GPU.
            - config: YAML formatted string containing the config to pass to focus.py.
            - queue: Name of the PCM queue to submit the job to (recommended is default).
        """
        ts, folder = self.get_str_time()
        jt = self.get_job('alos2_to_rslc')
        jt.set_input_params({
            'data_link': data_link,
            'gpu_enabled': '1' if gpu_enabled else '0',
            'timestamp': ts,
        })
        ret = isce3_regex.RSLC_FORMAT.format(
            polarization=NON_INSAR_POLARIZATION,
            timestamp=ts)
        ret = f'{DEFAULT_PCM_STORAGE}/L1_L_RSLC/{folder}/{ret}'
        logger.info(f'Submitting ALOS-2 to RSLC conversion job for {data_link}... (storage: {ret})')
        self.job_set.append(jt.submit_job(queue=queue))
        self.num_jobs += 1
        return ret

    def run_l0b_to_rslc(self,
                        data_link: str,
                        gpu_enabled: bool=True,
                        config: str='',
                        queue: str='nisar-job_worker-sciflo-rslc') -> str:
        """Runs L0B to RSLC.

        Input Parameters:
            - data_link: S3 URL to the L0B data to be processed into RSLC.
            - gpu_enabled: Whether to run focus.py using the GPU.
            - config: YAML formatted string containing the config to pass to focus.py.
            - queue: Name of the PCM queue to submit the job to (recommended is default).
        """
        ts, folder = self.get_str_time()
        jt = self.get_job('l0b_to_rslc')
        jt.set_input_params({
            'data_link': data_link,
            'gpu_enabled': '1' if gpu_enabled else '0',
            'focus_config': str(config),
            'timestamp': ts,
        })
        ret = isce3_regex.RSLC_FORMAT.format(
            polarization=NON_INSAR_POLARIZATION,
            timestamp=ts)
        ret = f'{DEFAULT_PCM_STORAGE}/L1_L_RSLC/{folder}/{ret}'
        logger.info(f'Submitting L0B to RSLC conversion job for {data_link}... (storage: {ret})')
        self.job_set.append(jt.submit_job(queue=queue))
        self.num_jobs += 1
        return ret

    def run_rslc_to_gslc(self,
                         data_link: str,
                         dem: str,
                         gpu_enabled: bool=True,
                         config: str='',
                         queue: str='nisar-job_worker-sciflo-gslc') -> str:
        """Runs RSLC to GSLC.

        Input Parameters:
            - data_link: S3 URL to the L0B data to be processed into RSLC.
            - dem: S3 URL to the DEM to be processed.
            - gpu_enabled: Whether to run focus.py using the GPU.
            - config: YAML formatted string containing the config to pass to focus.py.
            - queue: Name of the PCM queue to submit the job to (recommended is default).
        """
        ts, folder = self.get_str_time()
        jt = self.get_job('rslc_to_gslc')
        jt.set_input_params({
            'data_link': data_link,
            'dem_s3_url': dem,
            'gpu_enabled': '1' if gpu_enabled else '0',
            'gslc_config': str(config),
            'timestamp': ts,
        })
        ret = isce3_regex.GSLC_FORMAT.format(
            polarization=NON_INSAR_POLARIZATION,
            timestamp=ts)
        ret = f'{DEFAULT_PCM_STORAGE}/L2_L_GSLC/{folder}/{ret}'
        logger.info(f'Submitting RSLC to GSLC conversion job for {data_link}... (storage: {ret})')
        self.job_set.append(jt.submit_job(queue=queue))
        self.num_jobs += 1
        return ret

    def run_rslc_to_gcov(self,
                        data_link: str,
                        dem: str,
                        gpu_enabled: bool=True,
                        config: str='',
                        queue: str='nisar-job_worker-sciflo-gcov') -> str:
        """Runs RSLC to GCOV.

        Input Parameters:
            - data_link: S3 URL to the L0B data to be processed into RSLC.
            - dem: S3 URL to the DEM to be processed.
            - gpu_enabled: Whether to run focus.py using the GPU.
            - config: YAML formatted string containing the config to pass to focus.py.
            - queue: Name of the PCM queue to submit the job to (recommended is default).
        """
        ts, folder = self.get_str_time()
        jt = self.get_job('rslc_to_gcov')
        jt.set_input_params({
            'data_link': data_link,
            'dem_s3_url': dem,
            'gpu_enabled': '1' if gpu_enabled else '0',
            'gcov_config': str(config),
            'timestamp': ts,
        })
        ret = isce3_regex.GCOV_FORMAT.format(
            polarization=NON_INSAR_POLARIZATION,
            timestamp=ts)
        ret = f'{DEFAULT_PCM_STORAGE}/L2_L_GCOV/{folder}/{ret}'
        logger.info(f'Submitting RSLC to GCOV conversion job for {data_link}... (storage: {ret})')
        self.job_set.append(jt.submit_job(queue=queue))
        self.num_jobs += 1
        return ret

    def run_rslc_to_insar(self,
                          rslc_1: str,
                          rslc_2: str,
                          dem: str,
                          watermask: str,
                          gpu_enabled: bool=True,
                          config: str='',
                          queue: str=None) -> str:
        """Runs RSLC to INSAR.

        Input Parameters:
            - rslc_1: S3 URL to the first RLSC to be processed.
            - rslc_2: S3 URL to the second RSLC to be processed.
            - dem: S3 URL to the DEM to be processed.
            - gpu_enabled: Whether to run focus.py using the GPU.
            - config: YAML formatted string containing the config to pass to insar.py.
            - queue: Name of the PCM queue to submit the job to (recommended is default).
        """
        ts, folder = self.get_str_time()
        jt = self.get_job('rslc_to_insar')
        jt.set_input_params({
            'rslc_1': rslc_1,
            'rslc_2': rslc_2,
            'dem_s3_url': dem,
            'watermask_s3_url': watermask,
            'gpu_enabled': '1' if gpu_enabled else '0',
            'insar_config': str(config),
            'timestamp': ts,
        })
        ret = isce3_regex.GUNW_FORMAT.format(
            polarization=DEFAULT_POLARIZATION,
            timestamp=ts)
        ret = f'{DEFAULT_PCM_STORAGE}/L2_L_GUNW/{folder}/{ret}'
        logger.info(f'Submitting INSAR conversion job for {rslc_1} and {rslc_2}... (storage: {ret})')

        queue_ext = 'gpu' if gpu_enabled else 'cpu'
        queue_final = queue if queue is not None else f'nisar-job_worker-sciflo-insar-{queue_ext}'
        self.job_set.append(jt.submit_job(queue=queue_final))
        self.num_jobs += 1
        return ret

    def run_static_workflow(self,
                            orbit_xml: str,
                            pointing_xml: str,
                            dem: str,
                            watermask: str,
                            dem_margin: float,
                            watermask_margin: float,
                            config: str='',
                            #queue: str='nisar-job_worker-sciflo-gcov') -> str:
                            queue: str='nisar-job_worker-large') -> str:
        """Runs Static Workflow.

        Input Parameters:
            - orbit_xml: Raw text of the orbit XML file.
            - pointing_xml: Raw text of the pointing XML file.
            - dem: S3 URL to the DEM to be processed.
            - watermask: S3 URL to the Watermask to be processed.
            - config: YAML formatted string containing the config to pass to focus.py.
            - queue: Name of the PCM queue to submit the job to (recommended is default).
        """
        ts, folder = self.get_str_time()
        jt = self.get_job('static_workflow')
        jt.set_input_params({
            'dem_vrt_s3_url': dem,
            'watermask_vrt_s3_url': watermask,
            'dem_margin': dem_margin,
            'watermask_margin': watermask_margin,
            'orbit_xml': str(orbit_xml),
            'pointing_xml': str(pointing_xml),
            'config_yml': str(config),
            'timestamp': ts,
        })
        ret = isce3_regex.RSLC_FORMAT.format(
            polarization=NON_INSAR_POLARIZATION,
            timestamp=ts)
        ret = f'{DEFAULT_PCM_STORAGE}/L1_L_RSLC/{folder}/{ret}'
        logger.info(f'Submitting static workflow job... (storage: {ret})')
        self.job_set.append(jt.submit_job(queue=queue))
        self.num_jobs += 1
        return ret

    def run_static_layers_onprem(self,
                                 track: int,
                                 frame: int,
                                 orbit_xml: str,
                                 pointing_xml: str,
                                 config: str='',
                                 queue: str='nisar-job_worker-onprem') -> str:
        """Runs Static Workflow on-premise..

        Input Parameters:
            - track: The relative orbit number of the frame to process.
            - frame: The frame number to process for.
            - orbit_xml: S3 URL of the orbit XML file.
            - pointing_xml: S3 URL of the pointing XML file.
            - config: YAML formatted string containing the config to pass to focus.py.
            - queue: Name of the PCM queue to submit the job to (recommended is default).
        """
        ts, folder = self.get_str_time()
        jt = self.get_job('static_layers_onprem')
        jt.set_input_params({
            'track': track,
            'frame': frame,
            'orbit_xml': str(orbit_xml),
            'pointing_xml': str(pointing_xml),
            'config_yml': str(config),
            'timestamp': ts,
        })
        ret = isce3_regex.RSLC_FORMAT.format(
            polarization=NON_INSAR_POLARIZATION,
            timestamp=ts)
        ret = f'{DEFAULT_PCM_STORAGE}/L1_L_RSLC/{folder}/{ret}'
        logger.info(f'Submitting static workflow onprem job... (storage: {ret})')
        self.job_set.append(jt.submit_job(queue=queue))
        self.num_jobs += 1
        return ret

    def run_test_local_dem(self,
                           track: int,
                           frame: int,
                           queue: str='nisar-job_worker-onprem') -> str:
        """Tests mounting a local DEM archive.

        Input Parameters:
            - track: int
            - frame: int
        """
        ts, folder = self.get_str_time()
        jt = self.get_job('test_local_dem')
        jt.set_input_params({
            'track': track,
            'frame': frame,
        })
        ret = isce3_regex.RSLC_FORMAT.format(
            polarization=NON_INSAR_POLARIZATION,
            timestamp=ts)
        ret = f'{DEFAULT_PCM_STORAGE}/L1_L_RSLC/{folder}/{ret}'
        logger.info(f'Submitting test DEM archive job... (storage: {ret})')
        self.job_set.append(jt.submit_job(queue=queue))
        self.num_jobs += 1
        return ret

    def get_job(self, job_name: str):
        """Gets the listed job type from Mozart and initializes it."""
        if self.jobs.get(job_name) is None:
            full_job_name = f'job-{job_name}:{self.version}'
            jt = self.mozart.get_job_type(full_job_name)
            jt.initialize()
            logger.debug(f'Initialized {full_job_name}: {jt}')
            queues = jt.get_queues()['queues']['queues']
            logger.debug(f'List of Available Queues:\n {queues}')
            self.jobs[job_name] = jt
        return self.jobs[job_name]

    @staticmethod
    def write_manifest(outfname: str, io_list: list, header=None):
        """Writes the io_list to outfile."""
        os.makedirs(os.path.dirname(outfname), exist_ok=True)
        with open(outfname, 'w', encoding='utf-8') as f:
            if header is not None:
                f.write(header.strip())
            for item in io_list:
                f.write('\n')
                f.write(str(item[0]))
                for i in range(1, len(item)):
                    f.write(f',{str(item[i])}')


    @staticmethod
    def copy_with_nisar_filenames(outdir: str, destdir: str):
        """Copies all .h5 files into a directory with their inferred
        nisar filenames to a destination directory.
        """
        parsed_outdir = urlparse(outdir)
        out_bucket = parsed_outdir.netloc
        out_path = parsed_outdir.path

        if out_path.startswith('/'):
            out_path = path[1:]

        for partial_s3_path in AWS.list_s3(out_bucket, out_path):
            if not partial_s3_path.endswith('.h5'):
                continue
            full_s3_path = f's3://{out_bucket}/{partial_s3_path}'
            dest_fname = renameH5Product.infer_nisar_filename(full_s3_path)
            subprocess.run(f'aws s3 cp {full_s3_path} {args.output_bucket}/{dest_fname}', shell=True, check=True)


# 1. Define a global variable for the S3 client
# This ensures it persists across calls within the same process
s3_client = None

def s3_init_worker():
    """
    Initializes the S3 client once per worker process.
    This prevents the overhead of creating a new session for every file.
    """
    global s3_client
    s3_client = boto3.client('s3')

def s3_upload_file(task):
    """
    Worker function to upload a single file.
    task: tuple of (local_path, bucket_name, s3_key)
    """
    local_path, bucket, key = task
    try:
        # Use the global client initialized in s3_init_worker
        s3_client.upload_file(local_path, bucket, key)
        return f"Uploaded: {key}"
    except Exception as e:
        return f"Failed to upload {local_path}: {str(e)}"

def s3_upload_directory(outdir_list: list[str], output_bucket: str):
    # Clean the bucket name (handle cases where input is "s3://my-bucket")
    bucket_name = output_bucket.replace("s3://", "").rstrip("/")

    upload_tasks = []

    # 2. Gather all files first (Task Generation)
    # We replicate the --recursive --exclude "*" --include "*.h5" logic here
    for link, outdir in outdir_list:
        logger.info(f'Scanning files for {link} -> {outdir}')

        for root, dirs, files in os.walk(outdir):
            for file in files:
                if file.endswith('.h5'):
                    local_path = os.path.join(root, file)

                    # Calculate key relative to outdir to maintain folder structure in S3
                    # e.g., /tmp/data/sub/file.h5 -> sub/file.h5
                    relative_path = os.path.relpath(local_path, start=outdir)
                    s3_key = relative_path

                    upload_tasks.append((local_path, bucket_name, s3_key))

    logger.info(f"Found {len(upload_tasks)} .h5 files to upload. Starting pool...")

    # 3. Process uploads in parallel (Task Execution)
    # initializer=s3_init_worker ensures the client is created once per core
    with Pool(processes=cpu_count(), initializer=s3_init_worker) as pool:
        results = pool.map(s3_upload_file, upload_tasks)

    # Check for silent failures
    errors = [res for res in results if res is not None]
    if errors:
        print(f"ERROR: {len(errors)} files failed to upload!")
        print(errors[0]) # Print the first error to debug
    else:
        print(f"Successfully uploaded {len(upload_tasks)} files.")

    logger.info("Upload complete.")
