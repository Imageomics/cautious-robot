# Helper functions for download

import json
import pandas as pd
import os
from PIL import Image


def log_response(log_data, index, image, url, response_code):
    # log status
    log_entry = {}
    log_entry["Image"] = image
    log_entry["file_url"] = url
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

def downsample_and_save_image(image_dir_path, image_name, downsample_dir_path, downsample_size, log_errors, i, url, error_log_filepath):
    if not os.path.exists(downsample_dir_path):
        os.makedirs(downsample_dir_path, exist_ok=False)
    
    try:
        img = Image.open(f"{image_dir_path}/{image_name}")
        img.resize((downsample_size, downsample_size)).save(f"{downsample_dir_path}/{image_name}")
    except Exception as e:
        print(e)
        log_errors = log_response(
            log_errors,
            index=i,
            image="downsized_" + image_name,
            url=url,
            response_code=str(e)
        )
        update_log(log=log_errors, index=i, filepath=error_log_filepath)