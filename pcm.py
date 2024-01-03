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

import otello

import notebook_pges.isce3_regex as isce3_regex


# Default settings
DEFAULT_BUCKET = 's3://nisar-st-data-ondemand/ALOS-1-data'
DEFAULT_PCM_STORAGE = 's3://nisar-st-rs-ondemand/products'
DEFAULT_REPO = 'https://github.com/jplzhan/alos-to-insar.git'
DEFAULT_VERSION = 'v1.5.6'
DEFAULT_BUILD_TICK_SECONDS = 30
DEFAULT_AWS_PROFILE = 'saml-pub'
DEFAULT_POLARIZATION = 'HH'


# The directory this script is being ran in
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


# Logger
log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(stream=sys.stdout, format=log_format, level=logging.INFO)
logger = logging.getLogger('pcm_logger')


class PCM:
    """Class for launching PCM jobs."""
    def __init__(self, repo=DEFAULT_REPO, version=DEFAULT_VERSION, rebuild=False, profile=DEFAULT_AWS_PROFILE):
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
        }
        
    def build_pcm(self, tick_rate:float = DEFAULT_BUILD_TICK_SECONDS, rebuild: bool=False):
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
        
    def wait_for_completion(self):
        """Wait for all submitted jobs in the job set to complete."""
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
                        output_bucket: str, 
                        gpu_enabled: bool=True,
                        config: str='',
                        queue: str='nisar-job_worker-sciflo-rslc') -> str:
        """Runs ALOS to RSLC.
        
        Input Parameters:
            - data_link: S3 URL to the ALOS-1 data to be processed into NISAR L0B and then RSLC.
            - output_bucket: S3 bucket location to upload the RLSC result to.
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
            polarization=DEFAULT_POLARIZATION,
            timestamp=ts)
        ret = f'{DEFAULT_PCM_STORAGE}/L1_L_RSLC/{folder}/{ret}'
        logger.info(f'Submitting ALOS to RSLC conversion job for {data_link}... (storage: {ret})')
        self.job_set.append(jt.submit_job(queue=queue))
        self.num_jobs += 1
        return ret
        
    def run_alos2_to_rslc(self,
                        data_link: str,
                        output_bucket: str, 
                        gpu_enabled: bool=True,
                        queue: str='nisar-job_worker-sciflo-rslc') -> str:
        """Runs ALOS-2 to RSLC.
        
        Input Parameters:
            - data_link: S3 URL to the ALOS-2 data to be processed into NISAR RSLC.
            - output_bucket: S3 bucket location to upload the RLSC result to.
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
            polarization=DEFAULT_POLARIZATION,
            timestamp=ts)
        ret = f'{DEFAULT_PCM_STORAGE}/L1_L_RSLC/{folder}/{ret}'
        logger.info(f'Submitting ALOS-2 to RSLC conversion job for {data_link}... (storage: {ret})')
        self.job_set.append(jt.submit_job(queue=queue))
        self.num_jobs += 1
        return ret

    def run_l0b_to_rslc(self,
                        data_link: str,
                        output_bucket: str, 
                        gpu_enabled: bool=True,
                        config: str='',
                        queue: str='nisar-job_worker-sciflo-rslc') -> str:
        """Runs L0B to RSLC.
        
        Input Parameters:
            - data_link: S3 URL to the L0B data to be processed into RSLC.
            - output_bucket: S3 bucket location to upload the RLSC result to.
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
            polarization=DEFAULT_POLARIZATION,
            timestamp=ts)
        ret = f'{DEFAULT_PCM_STORAGE}/L1_L_RSLC/{folder}/{ret}'
        logger.info(f'Submitting L0B to RSLC conversion job for {data_link}... (storage: {ret})')
        self.job_set.append(jt.submit_job(queue=queue))
        self.num_jobs += 1
        return ret

    def run_rslc_to_gslc(self,
                        data_link: str,
                        dem: str,
                        output_bucket: str, 
                        gpu_enabled: bool=True,
                        config: str='',
                        queue: str='nisar-job_worker-sciflo-rslc') -> str:
        """Runs RSLC to GSLC.
        
        Input Parameters:
            - data_link: S3 URL to the L0B data to be processed into RSLC.
            - dem: S3 URL to the DEM to be processed.
            - output_bucket: S3 bucket location to upload the GSLC result to.
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
            polarization=DEFAULT_POLARIZATION,
            timestamp=ts)
        ret = f'{DEFAULT_PCM_STORAGE}/L2_L_GSLC/{folder}/{ret}'
        logger.info(f'Submitting RSLC to GSLC conversion job for {data_link}... (storage: {ret})')
        self.job_set.append(jt.submit_job(queue=queue))
        self.num_jobs += 1
        return ret
    
    def run_rslc_to_gcov(self,
                        data_link: str,
                        dem: str,
                        output_bucket: str, 
                        gpu_enabled: bool=True,
                        config: str='',
                        queue: str='nisar-job_worker-sciflo-rslc') -> str:
        """Runs RSLC to GCOV.
        
        Input Parameters:
            - data_link: S3 URL to the L0B data to be processed into RSLC.
            - dem: S3 URL to the DEM to be processed.
            - output_bucket: S3 bucket location to upload the GSLC result to.
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
            polarization=DEFAULT_POLARIZATION,
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
                          output_bucket: str,
                          gpu_enabled: bool=True,
                          config: str='',
                          queue: str='nisar-job_worker-sciflo-insar') -> str:
        """Runs RSLC to INSAR.
        
        Input Parameters:
            - rslc_1: S3 URL to the first RLSC to be processed.
            - rslc_2: S3 URL to the second RSLC to be processed.
            - dem: S3 URL to the DEM to be processed.
            - output_bucket: S3 bucket location to upload the GUNW result to.
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
            'gpu_enabled': '1' if gpu_enabled else '0',
            'insar_config': str(config),
            'timestamp': ts,
        })
        ret = isce3_regex.GUNW_FORMAT.format(
            polarization=DEFAULT_POLARIZATION,
            timestamp=ts)
        ret = f'{DEFAULT_PCM_STORAGE}/L2_L_GUNW/{folder}/{ret}'
        logger.info(f'Submitting INSAR conversion job for {rslc_1} and {rslc_2}... (storage: {ret})')
        self.job_set.append(jt.submit_job(queue=queue))
        self.num_jobs += 1
        return ret
        
    def get_job(self, job_name: str):
        """Gets the listed job type from Mozart and initializes it."""
        if self.jobs[job_name] is None:
            full_job_name = f'job-{job_name}:{self.version}'
            jt = self.mozart.get_job_type(full_job_name)
            jt.initialize()
            logger.debug(f'Initialized {full_job_name}: {jt}')
            queues = jt.get_queues()['queues']['queues']
            logger.debug(f'List of Available Queues:\n {queues}')
            self.jobs[job_name] = jt
        return self.jobs[job_name]
