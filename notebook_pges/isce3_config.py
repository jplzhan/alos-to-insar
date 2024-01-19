"""Contains helpers for configuring ISCE3 run configs and running RSLC or INSAR jobs."""
import os
import yaml
import asf_search as asf
import boto3
import aws_uploader
import zipfile

from urllib.parse import urlparse

from isce3_regex import *


def download_alos_data(urls: list, dl_dir: str, dest_dir: str) -> list:
    """Downloads ALOS-1 data given by the asf_search URL."""
    try:
        os.makedirs(dl_dir, exist_ok=True)
        os.makedirs(dest_dir, exist_ok=True)
        current_dir = os.getcwd()
        user_pass_session = asf.ASFSession().auth_with_creds(username, password)
        asf.download_urls(urls=urls, path=dl_dir, session=user_pass_session, processes=2)
        files = os.listdir(dl_dir)
        os.chdir(dest_dir)
        extract_dirs = []
        for f in files:
            splitext = os.path.splitext(f)
            zip_f = os.path.join(dl_dir, f)
            with zipfile.ZipFile(zip_f, 'r') as zip_ref:
                zip_ref.extractall(dest_dir)
            extract_dirs.append(os.path.abspath(splitext[0]))
        os.chdir(current_dir)
        print('Extracted:', extract_dirs)
        return extract_dirs
    except asf.ASFAuthenticationError as e:
        print(f'Auth failed: {e}')

def download_alos_s3_data(url: str, dl_dir: str, dest_dir: str) -> str:
    """Downloads ALOS-1 data given by an S3 url."""
    try:
        os.makedirs(dl_dir, exist_ok=True)
        os.makedirs(dest_dir, exist_ok=True)
        current_dir = os.getcwd()
        zip_f = os.path.join(dl_dir, os.path.basename(urlparse(url).path))
        if not os.path.exists(zip_f):
            print(f'Downloading {zip_f} from S3 bucket at {url}...')
            aws_uploader.AWS.download_s3(url, zip_f)
        else:
            print(f'{zip_f} already exists, skipping download...')
        os.chdir(dest_dir)
        extract_dir = os.path.join(dest_dir, os.path.basename(os.path.splitext(zip_f)[0]))
        with zipfile.ZipFile(zip_f, 'r') as zip_ref:
            zip_ref.extractall(dest_dir)
        os.chdir(current_dir)
        if os.path.isdir(extract_dir):
            print('Extracted:', extract_dir)
        else:
            raise ValueError(f'Failed to extract {extract_dir}!')
        return extract_dir
    except Exception as e:
        print(f'Exception caught while downloading ALOS-1 Data from S3: {e}')
        
def download_alos2_s3_data(url: str, dl_dir: str, dest_dir: str) -> str:
    """Downloads ALOS-2 data given by an S3 url."""
    try:
        os.makedirs(dl_dir, exist_ok=True)
        os.makedirs(dest_dir, exist_ok=True)
        current_dir = os.getcwd()
        zip_f = os.path.join(dl_dir, os.path.basename(urlparse(url).path))
        if not os.path.exists(zip_f):
            print(f'Downloading {zip_f} from S3 bucket at {url}...')
            aws_uploader.AWS.download_s3(url, zip_f)
        else:
            print(f'{zip_f} already exists, skipping download...')
        os.chdir(dest_dir)
        extract_dir = os.path.join(dest_dir, os.path.basename(os.path.splitext(zip_f)[0]))
        with zipfile.ZipFile(zip_f, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        os.chdir(current_dir)
        if os.path.isdir(extract_dir):
            print('Extracted:', extract_dir)
        else:
            raise ValueError(f'Failed to extract {extract_dir}!')
        return extract_dir
    except Exception as e:
        print(f'Exception caught while downloading ALOS-2 Data from S3: {e}')

def download_dem(url: str, dl_dir: str) -> str:
    """Downloads a DEM TIFF file given by an S3 url to the specified directory."""
    try:
        os.makedirs(dl_dir, exist_ok=True)
        tiff_f = os.path.join(dl_dir, os.path.basename(urlparse(url).path))
        if not os.path.exists(tiff_f):
            print(f'Downloading {tiff_f} from S3 bucket at {url}...')
            aws_uploader.AWS.download_s3(url, tiff_f)
        else:
            print(f'{tiff_f} already exists, skipping download...')

        if os.path.exists(tiff_f):
            print('Downloaded DEM:', tiff_f)
        else:
            raise ValueError(f'Failed to download {tiff_f}!')
        return tiff_f
    except Exception as e:
        print(f'Exception caught while downloading DEM Data from S3: {e}')

def write_focus_config(template: dict, target_path: str, yml_path: str, gpu_enabled: bool, outfile: str):
    """Writes a focus.py runconfig with the specified target path."""
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    template['runconfig']['groups']['worker']['gpu_enabled'] = gpu_enabled
    template['runconfig']['groups']['input_file_group']['input_file_path'] = [target_path]
    template['runconfig']['groups']['product_path_group']['sas_output_file'] = outfile
    template['runconfig']['groups']['product_path_group']['sas_config_file'] = yml_path
    with open(yml_path, 'w', encoding='utf-8') as f:
        yaml.dump(template, f, default_flow_style=False)

def write_gslc_config(template: dict, target_path: str, dem: str, yml_path: str, gpu_enabled: bool, outfile: str):
    """Writes a gslc.py runconfig with the specified target path."""
    template['runconfig']['groups']['worker']['gpu_enabled'] = gpu_enabled
    template['runconfig']['groups']['input_file_group']['input_file_path'] = target_path
    template['runconfig']['groups']['product_path_group']['sas_output_file'] = outfile
    # template['runconfig']['groups']['product_path_group']['sas_config_file'] = yml_path
    template['runconfig']['groups']['dynamic_ancillary_file_group']['dem_file'] = dem
    with open(yml_path, 'w', encoding='utf-8') as f:
        yaml.dump(template, f, default_flow_style=False)

def write_insar_config(template: dict, f1: str, f2: str, dem: str, yml_path: str, gpu_enabled: bool, outfile: str):
    """Writes the INSAR runconfig with the two specified RSLCs and a DEM path."""
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    template['runconfig']['groups']['worker']['gpu_enabled'] = gpu_enabled
    template['runconfig']['groups']['input_file_group']['reference_rslc_file'] = f1
    template['runconfig']['groups']['input_file_group']['secondary_rslc_file'] = f2
    template['runconfig']['groups']['dynamic_ancillary_file_group']['dem_file'] = dem
    template['runconfig']['groups']['product_path_group']['sas_output_file'] = outfile
    template['runconfig']['groups']['product_path_group']['product_path'] = os.path.dirname(outfile)

    # product_types = ['rifg', 'runw', 'gunw', 'roff', 'goff']
    # for p_type in product_types:
    #     p_type_upper = p_type.upper()
    #     template['runconfig']['groups']['input_file_group'][f'qa_{p_type}_input_file'] = f'{b1}_{b2}_{p_type_upper}.h5'
    
    with open(yml_path, 'w', encoding='utf-8') as f:
        yaml.dump(template, f, default_flow_style=False)

def write_gcov_config(template: dict, target_path: str, dem: str, yml_path: str, gpu_enabled: bool, outfile: str):
    """Writes a gcov.py runconfig with the specified target path."""
    template['runconfig']['groups']['worker']['gpu_enabled'] = gpu_enabled
    template['runconfig']['groups']['input_file_group']['input_file_path'] = target_path
    template['runconfig']['groups']['product_path_group']['sas_output_file'] = outfile
    template['runconfig']['groups']['dynamic_ancillary_file_group']['dem_file'] = dem
    with open(yml_path, 'w', encoding='utf-8') as f:
        yaml.dump(template, f, default_flow_style=False)
        
def prepend_env_var(env_var: str, val: str) -> str:
    """Prepends a value to an environment variable without crashing."""
    if os.environ.get(env_var, '').find(val) == -1:
        os.environ[env_var] = val + ':' + os.environ.get(env_var, '')
        if os.environ[env_var].endswith(':'):
            os.environ[env_var] = os.environ[env_var][:-1]
        return os.environ[env_var]
