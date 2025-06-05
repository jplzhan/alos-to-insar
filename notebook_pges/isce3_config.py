"""Contains helpers for configuring ISCE3 run configs and running RSLC or INSAR jobs."""
import os
import yaml
import asf_search as asf
import boto3
import aws_uploader
import zipfile
import json
import h5py
import shutil
import shapely
import math
import numpy as np

from osgeo import gdal
from urllib.parse import urlparse
from dataclasses import dataclass

from isce3_regex import *


gdal.UseExceptions()


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
    """Downloads a DEM TIFF file given by an S3 url to the specified directory.

    This actually works for the Watermask file as well.
    """
    try:
        os.makedirs(dl_dir, exist_ok=True)
        tiff_f = os.path.join(dl_dir, os.path.basename(urlparse(url).path))
        if not os.path.exists(tiff_f):
            print(f'Downloading {tiff_f} from S3 bucket at {url}...')
            aws_uploader.AWS.download_s3(url, tiff_f)
        else:
            print(f'{tiff_f} already exists, skipping download...')

        if os.path.exists(tiff_f):
            print('Downloaded DEM/Watermask:', tiff_f)
        else:
            raise ValueError(f'Failed to download {tiff_f}!')
        return tiff_f
    except Exception as e:
        print(f'Exception caught while downloading DEM/Watermask Data from S3: {e}')

def write_focus_config(template: dict, target_path: str, dem: str, yml_path: str, gpu_enabled: bool, outfile: str):
    """Writes a focus.py runconfig with the specified target path."""
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    template['runconfig']['groups']['worker']['gpu_enabled'] = gpu_enabled
    template['runconfig']['groups']['input_file_group']['input_file_path'] = [target_path]
    template['runconfig']['groups']['dynamic_ancillary_file_group']['dem_file'] = dem
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

def write_insar_config(template: dict,
                       f1: str,
                       f2: str,
                       dem: str,
                       watermask: str,
                       yml_path: str,
                       gpu_enabled: bool,
                       outfile: str):
    """Writes the INSAR runconfig with the two specified RSLCs and a DEM path."""
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    template['runconfig']['groups']['worker']['gpu_enabled'] = gpu_enabled
    template['runconfig']['groups']['input_file_group']['reference_rslc_file'] = f1
    template['runconfig']['groups']['input_file_group']['secondary_rslc_file'] = f2
    template['runconfig']['groups']['dynamic_ancillary_file_group']['dem_file'] = dem
    template['runconfig']['groups']['dynamic_ancillary_file_group']['water_mask_file'] = watermask
    template['runconfig']['groups']['product_path_group']['sas_output_file'] = outfile
    template['runconfig']['groups']['product_path_group']['scratch_path'] = 'scratch'
    template['runconfig']['groups']['product_path_group']['product_path'] = os.path.dirname(outfile)
    template['runconfig']['groups']['logging'] = {
        'path': 'insar.log',
        'write_mode': 'w',
    }

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

    # Specify the GCOV EPSG based on the target_path
    template['runconfig']['groups']['processing']['geocode']['output_epsg'] = h5parse.get_dem_epsg(dem)

    with open(yml_path, 'w', encoding='utf-8') as f:
        yaml.dump(template, f, default_flow_style=False)

def write_static_config(template: dict, dem_vrt: str, watermask_vrt: str, yml_path: str):
    """Writes a static.py runconfig with the specified target path."""
    os.makedirs(os.path.dirname(yml_path), exist_ok=True)

    template['runconfig']['groups']['dynamic_ancillary_file_group']['dem_raster_file'] = dem_vrt
    template['runconfig']['groups']['dynamic_ancillary_file_group']['water_mask_raster_file'] = watermask_vrt
    template['runconfig']['groups']['dynamic_ancillary_file_group']['orbit_xml_file'] = os.path.join(os.path.dirname(yml_path), 'orbit_final.xml')
    template['runconfig']['groups']['dynamic_ancillary_file_group']['pointing_xml_file'] = os.path.join(os.path.dirname(yml_path), 'pointing_final.xml')
    
    with open(yml_path, 'w', encoding='utf-8') as f:
        yaml.dump(template, f, default_flow_style=False)
        
def prepend_env_var(env_var: str, val: str) -> str:
    """Prepends a value to an environment variable without crashing."""
    if os.environ.get(env_var, '').find(val) == -1:
        os.environ[env_var] = val + ':' + os.environ.get(env_var, '')
        if os.environ[env_var].endswith(':'):
            os.environ[env_var] = os.environ[env_var][:-1]
        return os.environ[env_var]


@dataclass
class BoundingBox:
    """Simple class for storing the extent of a bounding box."""
    west: float = 0
    south: float = 0
    east: float = 0
    north: float = 0

    @staticmethod
    def load_from_geogrid(geo_grid: dict) -> object:
        """Reads in a geo_grid dict from a config and creates a bounding box object."""
        ret = BoundingBox(
            west = geo_grid['top_left']['x'],
            south = geo_grid['bottom_right']['y'],
            east = geo_grid['bottom_right']['x'],
            north = geo_grid['top_left']['y']
        )
        return ret


class h5parse:
    """Parses the h5py fields to help determine an inferred NISAR name."""
    @staticmethod
    def filename_parse(input_file):
        """Get filename function."""
        # Get the filename from the file path
        filename_only = os.path.basename(input_file)

        # Parse the filename using "_" as a delimiter and store each field into a list
        filename_parts = filename_only.split('_')

        # Print the list of fields
        print(filename_parts)
        return(filename_parts)

    # Function to search .h5 object for specified object name and return value
    def search_object(h5_obj, target_object):
        """
        Recursively search for a specific object in an HDF5 object and print its value if found.

        Parameters:
        - h5_obj: HDF5 group or dataset
        - target_object: Name of the object to search for
        """

        if isinstance(h5_obj, h5py.Group):
            # Iterate over the keys (group and dataset names) in the group
            for key in h5_obj.keys():
                # Recursively call h5parse.search_object on each subgroup or dataset
                result = h5parse.search_object(h5_obj[key], target_object)
                if result is not None:
                    return result
        elif isinstance(h5_obj, h5py.Dataset):
            # Check if the dataset name matches the target object
            if h5_obj.name.endswith(target_object):

                # Print the dataset value
                print(f"Value of '{target_object}': {h5_obj[()]}")
                print("h5_obj datatype: ")
                print(h5_obj[()].dtype)
                if (h5_obj[()].dtype in ("|S29","|S4", "|S26")):
                    print("BYTES FOUND")
                    objectByteValue = h5_obj[()]
                    print(f"Value of objectByteValue: {objectByteValue}")            
                    objectStrValue = f"{objectByteValue.decode('UTF-8')}"
                    print(f"Value of objectStrValue: {objectStrValue}")
                    return objectStrValue
                else:
                    objectValue = h5_obj[()]
                    print(f"Value of objectValue: {objectValue}")
                    objectStrValue = f"{objectValue}"
                    return objectStrValue

    @staticmethod
    def get_dem_epsg(dem_fname) -> BoundingBox:
        """Returns the UTM zone of an DEM input file."""
        src = gdal.Open(dem_fname)
        ulx, xres, xskew, uly, yskew, yres  = src.GetGeoTransform()
        lrx = ulx + (src.RasterXSize * xres)
        lry = uly + (src.RasterYSize * yres)

        x1,y1,x2,y2 = math.floor(ulx),math.floor(uly),math.floor(lrx),math.floor(lry)
        zone = int(np.ceil((ulx + 180)/6))

        if y1 >= 0:
           return 32600 + zone
        elif y1 < 0:
           return 32700 + zone

    @staticmethod
    def get_bounds(h5_fname) -> BoundingBox:
        """Returns the extent of a given h5 file, expected to be L0B or RSLC."""
        with h5py.File(h5_fname, 'r') as h5_file:
            a_group_key = list(h5_file.keys())[0]
            bbox = h5_file[a_group_key]['LSAR']['identification']['boundingPolygon'][()]
            bbox = bbox.decode('utf-8')
            polygon = shapely.wkt.loads(bbox)
            xx, yy = polygon.envelope.exterior.coords.xy
            ret = BoundingBox(west=min(xx), south=min(yy), east=max(xx), north=max(yy))
        return ret

    @staticmethod
    def infer_nisar_name(input_file: str) -> str:
        """Attempts to determine"""
        if not os.path.exists(input_file) or not input_file.endswith('.h5'):
            raise ValueError(f'{input_file} is not an existing .h5 file!')

        with h5py.File(input_file, 'r') as h5_obj:
            # Attempt to infer frame number
            try:
                param_frame_number = h5parse.search_object(h5_obj, "frameNumber")
                # Attempt to convert the string frame number into an int and reformat it back
                if isinstance(param_frame_number, str):
                    param_frame_number = int(param_frame_number)
                frame_number = "{:03d}".format(param_frame_number)
            except Exception as e:
                print(f"An error occurred while inferring the frame number: {e}")
                frame_number = "013"
                print(f"Using hardcoded frame number instead: {frame_number}")

            # Format name based on product type
            product_type = h5parse.search_object(h5_obj, "productType")
            if product_type in ["RSLC"]:
                new_filename= ("NISAR_" \
                    +"L1" \
                    + "_" + "PR" \
                    + "_" + product_type \
                    + "_" + "001" \
                    + "_" + "001" \
                    + "_" + "A" \
                    + "_" + frame_number \
                    + "_" + "2000" \
                    + "_" + "SHNA" \
                    + "_" + "A" \
                    + "_" + h5parse.search_object(
                        h5_obj, "zeroDopplerStartTime").replace('-', '').replace(':', '')[:15] \
                    + "_" + h5parse.search_object(
                        h5_obj, "zeroDopplerEndTime").replace('-', '').replace(':', '')[:15] \
                    + "_" + "T00888" \
                    + "_" + "M" \
                    + "_" + "F" \
                    + "_" + "J" \
                    + "_" + "888" \
                    + ".h5")
            elif product_type in ["GSLC", "GCOV"]:
                new_filename= ("NISAR_" \
                    +"L2" \
                    + "_" + "PR" \
                    + "_" + product_type \
                    + "_" + "001" \
                    + "_" + "001" \
                    + "_" + "A" \
                    + "_" + frame_number \
                    + "_" + "2000" \
                    + "_" + "SHNA" \
                    + "_" + "A" \
                    + "_" + h5parse.search_object(
                        h5_obj, "zeroDopplerStartTime").replace('-', '').replace(':', '')[:15] \
                    + "_" + h5parse.search_object(
                        h5_obj, "zeroDopplerEndTime").replace('-', '').replace(':', '')[:15] \
                    + "_" + "T00777" \
                    + "_" + "M" \
                    + "_" + "F" \
                    + "_" + "J" \
                    + "_" + "777" \
                    + ".h5")    
            elif product_type in ["RIFG", "RUNW", "ROFF"]:
                new_filename= ("NISAR_" \
                    +"L1" \
                    + "_" + "PR" \
                    + "_" + product_type \
                    + "_" + "001" \
                    + "_" + "001" \
                    + "_" + "A" \
                    + "_" + frame_number \
                    + "_" + "013" \
                    + "_" + "2000" \
                    + "_" + "SH" \
                    + "_" + h5parse.search_object(
                        h5_obj, "referenceZeroDopplerStartTime").replace('-', '').replace(':', '')[:15] \
                    + "_" + h5parse.search_object(
                        h5_obj, "referenceZeroDopplerEndTime").replace('-', '').replace(':', '')[:15] \
                    + "_" + h5parse.search_object(
                        h5_obj, "secondaryZeroDopplerStartTime").replace('-', '').replace(':', '')[:15] \
                    + "_" + h5parse.search_object(
                        h5_obj, "secondaryZeroDopplerEndTime").replace('-', '').replace(':', '')[:15] \
                    + "_" + "T00888" \
                    + "_" + "M" \
                    + "_" + "F" \
                    + "_" + "J" \
                    + "_" + "888" \
                    + ".h5")
            elif product_type in ["GUNW", "GOFF"]:
                new_filename= ("NISAR_" \
                    +"L2" \
                    + "_" + "PR" \
                    + "_" + product_type \
                    + "_" + "001" \
                    + "_" + "001" \
                    + "_" + "A" \
                    + "_" + "001" \
                    + "_" + frame_number \
                    + "_" + "2000" \
                    + "_" + "SH" \
                    + "_" + h5parse.search_object(
                        h5_obj, "referenceZeroDopplerStartTime").replace('-', '').replace(':', '')[:15] \
                    + "_" + h5parse.search_object(
                        h5_obj, "referenceZeroDopplerEndTime").replace('-', '').replace(':', '')[:15] \
                    + "_" + h5parse.search_object(
                        h5_obj, "secondaryZeroDopplerStartTime").replace('-', '').replace(':', '')[:15] \
                    + "_" + h5parse.search_object(
                        h5_obj, "secondaryZeroDopplerEndTime").replace('-', '').replace(':', '')[:15] \
                    + "_" + "T00888" \
                    + "_" + "M" \
                    + "_" + "F" \
                    + "_" + "J" \
                    + "_" + "888" \
                    + ".h5")
            elif (h5parse.search_object(h5_obj, "Soil_moisture")) is not None:
                new_filename= ("NISAR_" \
                    +"L3" \
                    + "_" + "PR" \
                    + "_" + "SME2" \
                    + "_" + "001" \
                    + "_" + "001" \
                    + "_" + "A" \
                    + "_" + frame_number \
                    + "_" + "2000" \
                    + "_" + "SHNA" \
                    + "_" + "A" \
                    + "_" + h5parse.search_object(
                        h5_obj, "zeroDopplerStartTime").replace('-', '').replace(':', '')[:15] \
                    + "_" + h5parse.search_object(
                        h5_obj, "zeroDopplerEndTime").replace('-', '').replace(':', '')[:15] \
                    + "_" + "T00888" \
                    + "_" + "M" \
                    + "_" + "F" \
                    + "_" + "J" \
                    + "_" + "888" \
                    + ".h5")

        return new_filename
