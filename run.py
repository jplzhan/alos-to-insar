import configparser
import logging
import boto3
import shutil
import argparse
import json
import os
import time
from urllib.parse import urlparse
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError

import otello

class AWS:
    def __init__(self, key: str, secret: str, token: str, region: str, configdir=None):
        if configdir is None:
            self.session = boto3.Session(
                aws_access_key_id=key,
                aws_secret_access_key=secret,
                aws_session_token=token,
                region_name=region
            )
            self.client = self.session.client('s3')
        else:
            if isinstance(configdir, str) and os.path.isdir(configdir):
                shutil.copytree(configdir, os.path.join(os.path.expanduser('~'), '.aws'))
            self.session = None
            self.client = boto3.client('s3')

    def upload_file(self, file_name: str, bucket: str, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        # Upload the file
        try:
            response = self.client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def upload_dir(self, dirname: str, bucket: str, path: str):
        """Recursively uploads a directory to an S3 bucket."""
        if not os.path.isdir(dirname):
            print('{} is not a directory!'.format(dirname))

        if not dirname.endswith('/'):
            dirname += '/'
        prefix_len = len(dirname)
        for parent, dirs, filenames in os.walk(dirname):
            for fname in filenames:
                obj_name = os.path.join(path, fname)
                if len(parent) > prefix_len:
                    obj_name = os.path.join(parent[prefix_len:], obj_name)
                obj_name = obj_name.lstrip('/')
                self.upload_file(os.path.join(parent, fname), bucket, obj_name)

    @staticmethod
    def list_s3(url: str) -> list:
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket(url)

        ret = []
        for object_summary in my_bucket.objects.filter(Prefix=''):
            ret.append(object_summary.key)
        return ret

    @staticmethod
    def download_s3(url: str, dest_file: str, cred: dict = None) -> str:
        """Download a file from an S3 URL.
        Args:
            url (str): S3 URL of input file.
            dest_file (str): Absolute file path to download towards.
            cred (dict): The credential dictionary to be passed into the boto3.client.
        Returns:
            str: relative path to the staged-in input file
        """

        # Create the parent directory if it does not exist
        os.makedirs(os.path.dirname(dest_file), exist_ok=True)

        if cred is None:
            s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        else:
            s3 = boto3.client('s3', **cred)

        # download input file
        p = urlparse(url)
        s3.download_file(p.netloc, p.path[1:], dest_file)

        return dest_file
    
    @staticmethod
    def s3_url_exists(url: str) -> bool:
        """Returns whether or not the specified path exists on S3."""
        parsed_url = urlparse(url)
        bucket = parsed_url.netloc
        path = parsed_url.path
        
        return path[1:] in AWS.list_s3(parsed_url.netloc)



def main() -> int:
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Generate science planning cleanup sequences",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-r",
        "--rebuild",
        action="store_true",
        help="Force rebuild the job even if the last build was a success."
    )
    args = parser.parse_args()
    
    bucket_name = 'nisar-adt-cc-ondemand'
    basefile_names = [
        # 'ALPSRP099950710-L1.0.zip',
        # 'ALPSRP106660710-L1.0.zip',
        # 'ALPSRP167050710-L1.0.zip',
        # 'ALPSRP207310710-L1.0.zip',
        # 'ALPSRP220730710-L1.0.zip',
        'ALPSRP267700710-L1.0.zip',
        'ALPSRP274410710-L1.0.zip',
    ]
    s3_urls = [f's3://{bucket_name}/ALOS-1-data/{b}' for b in basefile_names]
    
    # Read in AWS credentials
    config.read(f'{os.environ["HOME"]}/.aws/credentials')
    cred = config['saml-pub']

    for basefile_name in basefile_names:
        basefile = f'/home/jovyan/otello/data/{basefile_name}'
        s3_url = f's3://{bucket_name}/ALOS-1-data/{basefile_name}'
        s3_objects = AWS.list_s3(bucket_name)
        parsed_url = urlparse(s3_url)
        bucket = parsed_url.netloc
        path = parsed_url.path

        if path[1:] not in s3_objects:
            print(f'========={path[1:]} not found in S3 object paths: {s3_objects}===========')
            print(f'Now uploading {basefile} to bucket {bucket}...')
            uploader = AWS(cred['aws_access_key_id'], cred['aws_secret_access_key'], cred['aws_session_token'], cred['region'])
            uploader.upload_file(basefile, bucket, path)
        else:
            print(f'{path[1:]} found in S3 object paths, now continuing...\n')

    # Jenkins docker build
    branch = "v1.1"
    ci = otello.CI(repo="https://github.com/jplzhan/alos-to-insar.git", branch=branch)
        
    try:
        ci.check_job_exists()
    except Exception as e:
        print("Job did not exist previously. Now registering job...")
        ci.register()
    
    try:
        build_status = ci.get_build_status()
        print(build_status)
        if build_status["result"] == "FAILURE" or args.rebuild:
            print("Last build failed, now rebuilding...")
            status = ci.submit_build()
            print(status)
            time.sleep(10)
    except Exception as e:
        print("Exception caught:", e)
        print("CI job was most likely never built, now submitting a build...")
        status = ci.submit_build()
        time.sleep(10)

    # Wait for the job to finish building
    build_status = ci.get_build_status()
    while build_status["building"]:
        print(build_status)
        print("<<<<<<<Job is still building, please wait...>>>>>>")
        time.sleep(30)
        build_status = ci.get_build_status()

    if build_status["result"] == "FAILURE":
        print("\n<<<Submitted build failed, docker build must have crashed. Please check URL>>>\n")
        return 1

    # Mozart
    m = otello.Mozart()
    
    #job_name = f"job-alos_to_rslc:{branch}"
    job_name = f"job-rslc_to_insar:{branch}"
    
    j_types = m.get_job_types()
    for k in j_types.keys():
        print(k)
            
    try:
        # returns the JobType instance representing the specified job
        jt = m.get_job_type(job_name)
        print(jt)
    except Exception as e:
        print("Exception caught trying to get job type:", e)

    # retrieving the Job wiring and parameters
    jt.initialize()

    # retrieving and setting the job queues
    queues = jt.get_queues()
    print('List of Queues:', queues['queues']['queues'])
    
    rslc_queue = 'nisar-job_worker-sciflo-rslc'
    insar_queue = 'nisar-job_worker-sciflo-insar'
    if rslc_queue not in queues['queues']['queues']:
        print(f'Cannot find queue with name "{rslc_queue} in list of queues:"', queues['queues']['queues'])
        return 1
    if insar_queue not in queues['queues']['queues']:
        print(f'Cannot find queue with name "{insar_queue} in list of queues:"', queues['queues']['queues'])
        return 1
    
    jt.describe()

    # Submitting the job
    job_set = otello.JobSet()
    if True:
        config = configparser.ConfigParser()
        print('=========REMEMBER TO RUN "aws-login.linux.amd64" FOR A ROLE IN THE "saml-pub" PROFILE!!!!==========')
        if job_name.startswith('job-alos_to_rslc'):
            for s3_url in s3_urls:
                jt.set_input_params({
                    "data_link": s3_url,
                    "gpu_enabled": "1",
                    "s3_upload": "1",
                    'region': cred['region'],
                    'key': cred['aws_access_key_id'],
                    'secret': cred['aws_secret_access_key'],
                    'token': cred['aws_session_token'],
                })
                job_set.append(jt.submit_job(queue=rslc_queue))
        elif job_name.startswith('job-rslc_to_insar'):
            rslc_job_set = otello.JobSet()
            rslc_s3_urls = [f's3://nisar-adt-cc-ondemand/ALOS-1-data/RSLC/{os.path.splitext(b)[0]}.h5' for b in basefile_names]
            to_generate = []
            rslc_jt = m.get_job_type(f'job-alos_to_rslc:{branch}')
            rslc_jt.initialize()

            for alos_url, url in zip(s3_urls, rslc_s3_urls):
                if not AWS.s3_url_exists(url):
                    print(f'{url} does not exist, submitting RSLC job...')
                    rslc_jt.set_input_params({
                        'data_link': alos_url,
                        'gpu_enabled': "1",
                        's3_upload': "1",
                        'region': cred['region'],
                        'key': cred['aws_access_key_id'],
                        'secret': cred['aws_secret_access_key'],
                        'token': cred['aws_session_token'],
                    })
                    rslc_job_set.append(rslc_jt.submit_job(queue=rslc_queue))
                    to_generate.append(url)
            if len(rslc_job_set) > 0:
                rslc_job_set.wait_for_completion()
                for url in to_generate:
                    if AWS.s3_url_exists(url):
                        print('RLSC generated:', url)
                    else:
                        print('======== Failed to generate RLSC:', url)
                        return 1
            else:
                print('All RSLC S3 URLs exist, now continuing...')
            
            for i, rslc_1 in enumerate(rslc_s3_urls):
                if i >= len(rslc_s3_urls) - 1:
                    break
                for rslc_2 in rslc_s3_urls[i+1:]:
                    print(f'Submitting INSAR job for {os.path.basename(rslc_1)} and {os.path.basename(rslc_2)}...')
                    jt.set_input_params({
                        'rslc_1': f'{rslc_1}',
                        'rslc_2': f'{rslc_2}',
                        'dem_s3_url': 's3://nisar-adt-cc-ondemand/DEM-static/dem.tiff',
                        'gpu_enabled': '1',
                        's3_upload': '1',
                        'region': cred['region'],
                        'key': cred['aws_access_key_id'],
                        'secret': cred['aws_secret_access_key'],
                        'token': cred['aws_session_token'],
                    })
                    job_set.append(jt.submit_job(queue=insar_queue))

    job_set_status = job_set.wait_for_completion()
    print(job_set_status)
    print("\n============================= DONE ====================================\n")
    
    return 0
    
if __name__ == "__main__":
    main()
