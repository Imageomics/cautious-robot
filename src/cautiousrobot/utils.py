# Helper functions for download

import json
import sys
import pandas as pd
import os
from PIL import Image
from sumbuddy import gather_file_paths
from sumbuddy.exceptions import EmptyInputDirectoryError


def log_response(log_data, index, image, file_path, response_code):
    # log status
    log_entry = {}
    log_entry["Image"] = image
    log_entry["file_path"] = file_path
    log_entry["Response_status"] = str(response_code) #int64 has problems sometimes
    log_data[index] = log_entry

    return log_data


def update_log(log, index, filepath):
    # save logs
    with open(filepath, "a") as log_file:
        json.dump(log[index], log_file, indent = 4)
        log_file.write("\n")


def process_csv(csv_path, expected_cols):
    '''
    Reads a CSV, sets all columns to lowercase (for case-insensitivity), and checks for expected columns.
    
    Parameters:
    csv_path - String. Path to the CSV.
    expected_cols - Dictionary of column headers expected to be present in the CSV.

    Returns:
    df - DataFrame processed from the CSV.
    '''
    df = pd.read_csv(csv_path, low_memory = False)
    df.columns = df.columns.str.lower()

    missing_cols = []
    for col in list(expected_cols.keys()):
        if expected_cols[col] not in list(df.columns):
            missing_cols.append(col)
    if len(missing_cols) > 0:
        raise Exception(f"The CSV at {csv_path} is missing column(s): {missing_cols}, defined as {[expected_cols[col] for col in missing_cols]}.")
    
    return df

def downsample_and_save_image(image_dir_path, image_name, downsample_dir_path, downsample_size, log_errors, image_index, file_path, error_log_filepath):
    """
    Downsample an image and save it to the specified directory.

    Parameters:
    - image_dir_path (str): The path to the directory containing the original image.
    - image_name (str): The name of the image to be downsampled.
    - downsample_dir_path (str): The path to the directory where the downsampled image will be saved.
    - downsample_size (int): The new size (both width and height) for the downsampled image.
    - log_errors (dict): A dictionary to store errors encountered during the downsampling process.
    - image_index (int): The index of the current image being processed, used for logging.
    - file_path (str): The file path or URL associated with the image, used for logging errors.
    - error_log_filepath (str): The file path where error logs are stored.

    Returns:
    None
    """    
    if not os.path.exists(downsample_dir_path):
        os.makedirs(downsample_dir_path, exist_ok=False)
    
    try:
        img = Image.open(f"{image_dir_path}/{image_name}")
        img.resize((downsample_size, downsample_size)).save(f"{downsample_dir_path}/{image_name}")
    except Exception as e:
        print(e)
        log_errors = log_response(
            log_errors,
            index=image_index,
            image="downsized_" + image_name,
            file_path=file_path,
            response_code=str(e)
        )
        update_log(log=log_errors, index=image_index, filepath=error_log_filepath)
        
def check_existing_images(csv_path, img_dir, source_df, filename_col, subfolders = None):
    """
    Checks which files from the CSV already exist in the image directory.

    Adds a new boolean column `in_img_dir` to source_df indicating which images
    are already in the directory.

    If all images already exist in the directory, the function will exit early
    by calling `sys.exit()`, and no further processing will occur.

    Parameters:
        csv_path (str): Path to the CSV file containing image information.
        img_dir (str): Path to the directory where images are to be stored.
        source_df (pd.DataFrame): DataFrame loaded from the CSV, containing image metadata.
        filename_col (str): Name of the column in source_df that contains image filenames.
        subfolders (str): Name of the column in source_df that contains subfolder names. (optional)

    Returns:
        updated_df (pd.DataFrame): DataFrame with new column 'in_img_dir' indicating presence in img_dir.
        filtered_df (pd.DataFrame): DataFrame filtered to only files not present in img_dir.
    """
    # Create a copy to avoid modifying the original DataFrame
    df = source_df.copy()
    
    if not os.path.exists(img_dir):
        # Directory doesn't exist, so nothing to check
        df["in_img_dir"] = False
        
        # Return the updated df and the filtered dataframe of items that still need downloading
        filtered_df = df[~df["in_img_dir"]].copy()
        return df, filtered_df

    try:
        existing_files = gather_file_paths(img_dir)
    except EmptyInputDirectoryError:
        # If the directory exists but is empty, sumbuddy raises an error.
        # We catch it and treat it as an empty file list.
        existing_files = []
    
    existing_full_paths = {os.path.normpath(os.path.relpath(f, img_dir)) for f in existing_files}

    if subfolders:
        # We use a generic join here, but the apply(os.path.normpath) below fixes it for the specific OS
        raw_paths = df[subfolders].astype(str) + os.sep + df[filename_col].astype(str)

        # This converts '/' to '\' on Windows, or vice versa, ensuring a match
        df["expected_path"] = raw_paths.apply(os.path.normpath)
    else:
        # Normalize even simple filenames just in case they contain pathing characters
        df["expected_path"] = df[filename_col].astype(str).apply(os.path.normpath)
        
    # Determine which expected paths physically exist
    expected_present = df["expected_path"].isin(existing_full_paths)
    df["in_img_dir"] = expected_present.copy()
    
    # Clean up the temporary column before returning.
    df = df.drop(columns=["expected_path"])
    
    # Create filtered DataFrame
    filtered_df = df[~df["in_img_dir"]].copy()
    
    # Exit if all images are already there
    if filtered_df.empty:
        sys.exit(f"'{img_dir}' already contains all images. Exited without executing.")
    else:
        # Print directory status message - pre-download
        num_existing = len(existing_files)
        print(f"There are {num_existing} of the desired files already in {img_dir}. Based on {csv_path}, {filtered_df.shape[0]} images should be downloaded.")
        
    return df, filtered_df
