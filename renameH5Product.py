#!/usr/bin/env python
# coding: utf-8

# Copyright 2024, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government sponsorship acknowledged. Any commercial use must be negotiated with the Office of Technology Transfer at the California Institute of Technology.
# 
# This software may be subject to U.S. export control laws and regulations. By accepting this document, the user agrees to comply with all applicable U.S. export laws and regulations. User has the responsibility to obtain export licenses, or other export authority as may be required, before exporting such information to foreign countries or providing access to foreign persons.

# # Rename .h5 products to NISAR format based off of contents in .h5
# This script renames .h5 product file to support ingest into the PCM/DAAC systems. It extracts out contents from the .h5 to populate fields in the filename along with static fields hardcoded as needed to support basic ingest

import json
import h5py
import os
import argparse
import shutil

# Get filename function
def filename_parse(input_file):
    # Get the filename from the file path
    filename_only = os.path.basename(input_file)

    # Parse the filename using "_" as a delimiter and store each field into a list
    filename_parts = filename_only.split('_')

    # Print the list of fields
    print(filename_parts)
    return(filename_parts)

# .h5 metadata extraction functions
def openH5(input_h5product):
    with h5py.File(input_h5product, 'r') as h5_obj:
        return h5_obj
    
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
            # Recursively call search_object on each subgroup or dataset
            result = search_object(h5_obj[key], target_object)
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

# Entry point of execution

if __name__ == "__main__":
    # Parses the command line arguments.
     
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-f', '--file', required=True, help='Input .h5 file')
    parser.add_argument('-o', '--output', required=True, help='The output path of where renamed files should be placed. Make sure to include "/" at end of path. Example: /home/jovyan/output/')

    args = parser.parse_args()
    

    input_file = args.file
    renamed_path = args.output
    print("input_file::main:input_met {}".format(input_file))
    print("output_path::main:renamed_path {}".format(renamed_path))
    

    # Specify the input and output filenames
    #input_file = '/Users/jpon/Documents/Projects/GIT/nisar-DataTools/DAAC_Ondemand_Reformat/RSLC/input/Renamed_to_NISAR/ALPSRP274410710-L1.0.h5'  # Update with your HDF5 file path
    #renamed_path ='/Users/jpon/Documents/Projects/GIT/nisar-DataTools/DAAC_Ondemand_Reformat/GUNW/Output/'

    
    with h5py.File(input_file, 'r') as h5_obj:
        # Attempt to infer frame number
        try:
            param_frame_number = search_object(h5_obj, "frameNumber")
            # Attempt to convert the string frame number into an int and reformat it back
            if isinstance(param_frame_number, str):
                param_frame_number = int(param_frame_number)
            frame_number = "{:03d}".format(param_frame_number)
        except Exception as e:
            print(f"An error occurred while inferring the frame number: {e}")
            frame_number = "013"
            print(f"Using hardcoded frame number instead: {frame_number}")

        # Format name based on product type
        product_type = search_object(h5_obj, "productType")
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
                + "_" + search_object(h5_obj, "zeroDopplerStartTime").replace('-', '').replace(':', '')[:15] \
                + "_" + search_object(h5_obj, "zeroDopplerEndTime").replace('-', '').replace(':', '')[:15] \
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
                + "_" + search_object(h5_obj, "zeroDopplerStartTime").replace('-', '').replace(':', '')[:15] \
                + "_" + search_object(h5_obj, "zeroDopplerEndTime").replace('-', '').replace(':', '')[:15] \
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
                + "_" + search_object(h5_obj, "referenceZeroDopplerStartTime").replace('-', '').replace(':', '')[:15] \
                + "_" + search_object(h5_obj, "referenceZeroDopplerEndTime").replace('-', '').replace(':', '')[:15] \
                + "_" + search_object(h5_obj, "secondaryZeroDopplerStartTime").replace('-', '').replace(':', '')[:15] \
                + "_" + search_object(h5_obj, "secondaryZeroDopplerEndTime").replace('-', '').replace(':', '')[:15] \
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
                + "_" + frame_number \
                + "_" + "013" \
                + "_" + "2000" \
                + "_" + "SH" \
                + "_" + search_object(h5_obj, "referenceZeroDopplerStartTime").replace('-', '').replace(':', '')[:15] \
                + "_" + search_object(h5_obj, "referenceZeroDopplerEndTime").replace('-', '').replace(':', '')[:15] \
                + "_" + search_object(h5_obj, "secondaryZeroDopplerStartTime").replace('-', '').replace(':', '')[:15] \
                + "_" + search_object(h5_obj, "secondaryZeroDopplerEndTime").replace('-', '').replace(':', '')[:15] \
                + "_" + "T00888" \
                + "_" + "M" \
                + "_" + "F" \
                + "_" + "J" \
                + "_" + "888" \
                + ".h5")
        elif (search_object(h5_obj, "Soil_moisture")) is not None:
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
                + "_" + search_object(h5_obj, "zeroDopplerStartTime").replace('-', '').replace(':', '')[:15] \
                + "_" + search_object(h5_obj, "zeroDopplerEndTime").replace('-', '').replace(':', '')[:15] \
                + "_" + "T00888" \
                + "_" + "M" \
                + "_" + "F" \
                + "_" + "J" \
                + "_" + "888" \
                + ".h5")
                
    try:
        shutil.copy2(input_file, renamed_path + new_filename)
        print(f"File '{input_file}' renamed to '{new_filename}' successfully.")
    except FileNotFoundError:
        print(f"File '{input_file}' not found.")
    except FileExistsError:
        print(f"File '{new_filename}' already exists.")
    except Exception as e:
        print(f"An error occurred: {e}")
   