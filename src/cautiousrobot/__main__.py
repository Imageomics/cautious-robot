# Downloads images from CSV with option to save downsampled copies.
# Logs image downloads and failures in json files.
# Logs response codes as strings, not int64.

# Logs saved in same folder as CSV used for download
# Downsized images are saved in <img_dir>_downsized

import requests
import shutil
import pandas as pd
import argparse
import hashlib

from tqdm import tqdm
import os
import sys
import time
from sumbuddy import get_checksums
from cautiousrobot.utils import log_response, update_log, process_csv, downsample_and_save_image
from cautiousrobot.buddy_check import BuddyCheck


REDO_CODE_LIST = [429, 500, 502, 503, 504]


def parse_args():
    available_algorithms = ', '.join(hashlib.algorithms_available)

    parser = argparse.ArgumentParser()
    # Use argument groups for required vs optional (both get short flags too) https://bugs.python.org/issue9694#msg132327
    # Required arguments
    req_args = parser.add_argument_group("required arguments")
    req_args.add_argument("-i", "--input-file", required = True, help = "path to CSV file with urls.", nargs = "?")
    req_args.add_argument("-o", "--output-dir", required = True, help = "main directory to download images into.", nargs = "?")
    
    # Optional arguments
    opt_args = parser.add_argument_group("optional arguments")
    opt_args.add_argument("-s", "--subdir-col", required = False,
                        help = "name of column to use for subfolders in image directory (defaults to flat directory if left blank)",
                        nargs = "?")
    opt_args.add_argument("-n", "--img-name-col", default = "filename", help = "column to use for image filename (default: filename)", nargs = "?")
    opt_args.add_argument("-u", "--url-col", default = "file_url", help = "column with URLs to download (default: file_url)", nargs = "?")
    opt_args.add_argument("-w", "--wait-time", default = 3, help = "seconds to wait between retries for an image (default: 3)", type = int)
    opt_args.add_argument("-r", "--max-retries", default = 5, help = "max times to retry download on a single image (default: 5)", type = int)
    opt_args.add_argument("-l", "--side-length", required = False,
                        help = "number of pixels per side for resized square images (default: no resized images created)",
                        type = int)
    opt_args.add_argument("-x", "--starting-idx", default = 0, help = "index of CSV at which to start download (default: 0)", type = int)
    opt_args.add_argument("-a", "--checksum-algorithm", default = 'md5', #choices = available_algorithms,
                        help = f"checksum algorithm to use on images (default: md5, available: {available_algorithms})"
                        )
    opt_args.add_argument("-v", "--verifier-col", required = False, help = "name of column in source CSV with checksums (same hash as -a) to verify download", nargs = "?")
    
    return parser.parse_args()


def download_images(data, img_dir, log_filepath, error_log_filepath, filename = "filename",
                    subfolders = None, downsample_path = None, downsample = None,
                    file_url = "file_url", wait = 3, retry = 5, starting_index = 0):
    '''
    Download images to img_dir and downsampled images to a chosen downsized image path.

    Parameters:
    data - Dataframe with image metadata (must have filename and file_url).
    img_dir - String. Directory in which to save images.
    log_filepath - String. Filepath for the download log.
    error_log_filepath - String. Filepath for the download error log.
    filename - String. Name of column to use for image filenames. Default: 'filename'.
    subfolders - String [optional]. Name of column to use for subfolder designations. Defaults to flat directory if none provided.
    downsample_path - String [optional]. Folder to which to download the downsized images.
    downsample - Int [optional]. Number of pixels per side for downsampled images.
    file_url - String. Name of column to use for image urls. Default: 'file_url'.
    wait - Int. Seconds to wait between retries for an image. Default: 3.
    retry - Int. Max number of times to retry downloading an image. Default: 5.
    starting_index - Int. Index at which to start the download. Default: 0.
    
    '''
    log_data = {}
    log_errors = {}

    # Will attempt to download everything in CSV from starting_index
    for i in tqdm(data.index):
        if i < starting_index:
            continue
        image_dir_path = img_dir 
        image_name = data[filename][i]
        if subfolders:
            image_dir_path = img_dir + "/" + data[subfolders][i]

        if not os.path.exists(image_dir_path + "/" + image_name):
            
            # get image from url
            url = data[file_url][i]
            if not url:
                log_errors = log_response(log_errors,
                                        index = i,
                                        image = image_name,
                                        file_path = url,
                                        response_code = "no url")
                update_log(log = log_errors, index = i, filepath = error_log_filepath)
            
            else:
                #download the image
                redo = True
                max_redos = retry
                while redo and max_redos > 0:
                    try:
                        response = requests.get(url, stream = True)
                    except Exception as e:
                        redo = True
                        max_redos -= 1
                        if max_redos <= 0:
                            log_errors = log_response(log_errors,
                                            index = i,
                                            image = image_name,
                                            file_path = url,
                                            response_code = str(e))
                            update_log(log = log_errors, index = i, filepath = error_log_filepath)
                        continue
                            
                    if response.status_code == 200:
                        redo = False
                        # log status
                        log_data = log_response(log_data,
                                            index = i,
                                            image = image_name,
                                            file_path = url,
                                            response_code = response.status_code
                                            )
                        update_log(log = log_data, index = i, filepath = log_filepath)
                        
                        #create the appropriate folders if necessary
                        
                        if not os.path.exists(image_dir_path):
                            os.makedirs(image_dir_path, exist_ok=False)
                        
                        # save full size image to appropriate folder
                        with open(f"{image_dir_path}/{image_name}", "wb") as out_file:
                            shutil.copyfileobj(response.raw, out_file)

                    # check for too many requests
                    elif response.status_code in REDO_CODE_LIST:
                        redo = True
                        max_redos -= 1
                        if max_redos <= 0:
                            log_errors = log_response(log_errors,
                                            index = i,
                                            image = image_name,
                                            file_path= url,
                                            response_code = response.status_code)
                            update_log(log = log_errors, index = i, filepath = error_log_filepath)
    
                        else:
                            time.sleep(wait)
                    else: #other fail, eg. 404
                        redo = False
                        log_errors = log_response(log_errors,
                                                index = i,
                                                image = image_name,
                                                file_path = url,
                                                response_code = response.status_code)
                        update_log(log = log_errors, index = i, filepath = error_log_filepath)
    
                    del response
                
            if downsample:
                # Since we have image resize within a try and log failure, seems reasonable to not check for the image again.
                downsample_dir_path = downsample_path
                if subfolders:
                    downsample_dir_path = downsample_path + "/" + data[subfolders][i]
                if os.path.exists(downsample_dir_path + "/" + image_name):
                    # Don't overwrite resized images either
                    continue
                
                downsample_and_save_image(
                    image_dir_path=image_dir_path,
                    image_name=image_name,
                    downsample_dir_path=downsample_dir_path,
                    downsample_size=downsample,
                    log_errors=log_errors,
                    image_index=i,
                    file_path=url,
                    error_log_filepath=error_log_filepath
                )
                


    return


def main():
    args = parse_args()
    csv_path = args.input_file
    if not csv_path.endswith(".csv"):
        sys.exit("Expected CSV for input file; extension should be '.csv'")

    # Make case-insensitive & check for required columns
    subfolders = args.subdir_col
    expected_cols = {
        "filename_col": args.img_name_col.lower(),
        "url_col": args.url_col.lower()
        }
    if subfolders:
        subfolders = subfolders.lower()
        expected_cols["subfolders"] = subfolders
    try:
        data_df = process_csv(csv_path, expected_cols)
    except Exception as missing_cols:
        sys.exit(f"{missing_cols} Please adjust inputs and try again.")

    # Check for missing filenames & uniqueness
    filename_col = expected_cols["filename_col"]
    url_col = expected_cols["url_col"]
    if data_df.loc[data_df[filename_col].notna()].shape[0] != data_df[filename_col].nunique():
        sys.exit(f"{filename_col} is not a unique identifier for this dataset, please choose a column with unique values for filenames.")
    urls_no_name = len(data_df.loc[(data_df[filename_col].isna() & (data_df[url_col].notna()))])
    if urls_no_name > 0:
        ignore = input(f"'{filename_col}' is missing values for {urls_no_name} URLs. Proceed with download ignoring these URLs? [y/n]: ")
        if ignore.lower() != "y":
            sys.exit("Exited without executing.")
    
    # Set source DataFrame for only non-null filename values
    source_df = data_df.loc[data_df[filename_col].notna()].copy()

    # Check for img_dir
    img_dir = args.output_dir
    if os.path.exists(img_dir):
        sys.exit(f"'{img_dir}' already exists. Exited without executing.")

    # Set location for logs
    metadata_path = csv_path.split(".")[0]
    log_filepath = metadata_path + "_log.jsonl"
    error_log_filepath = metadata_path + "_error_log.jsonl"

    # Check for downsample
    if isinstance(args.side_length, int):
        downsample_dest_path = img_dir + "_downsized"
        # dowload images from urls & save downsample copy
        download_images(source_df,
                        img_dir = img_dir,
                        log_filepath = log_filepath,
                        error_log_filepath = error_log_filepath,
                        filename = filename_col,
                        subfolders = subfolders,
                        downsample_path = downsample_dest_path,
                        downsample = args.side_length,
                        file_url = url_col,
                        wait = args.wait_time,
                        retry = args.max_retries,
                        starting_index = args.starting_idx)
        print(f"Images downloaded from {csv_path} to {img_dir}, with downsampled images in {downsample_dest_path}.")

    else:
        # dowload images from urls without downsample copy
        download_images(source_df,
                        img_dir = img_dir,
                        log_filepath = log_filepath,
                        error_log_filepath = error_log_filepath,
                        filename = filename_col,
                        subfolders = subfolders,
                        file_url = url_col,
                        wait = args.wait_time,
                        retry = args.max_retries,
                        starting_index = args.starting_idx)
        print(f"Images downloaded from {csv_path} to {img_dir}.")
    print(f"Download logs are in {log_filepath} and {error_log_filepath}.")

    # generate checksums and save CSV to same folder as CSV used for download
    # then verify the download if checksums in source CSV
    checksum_path = metadata_path + "_checksums.csv"
    try:
        get_checksums(input_path = img_dir, output_filepath = checksum_path, algorithm = args.checksum_algorithm)
        
        # verify numbers
        checksum_df = pd.read_csv(checksum_path, low_memory = False)
        expected_num_imgs = source_df.shape[0]
        print(f"{checksum_df.shape[0]} images were downloaded to {img_dir} of the {expected_num_imgs} expected.")
    except Exception as e:
        print(f"checksum calculation of downloaded images was unsuccessful due to {e}.")
        print(f"you can get checksums for the images downloaded to {img_dir} by running sum-buddy directly.")
        return

    if args.verifier_col:
        # Run download verification
        buddy_check = BuddyCheck(buddy_id = "filename", buddy_col = args.checksum_algorithm)
        try:
            missing_imgs = buddy_check.validate_download(source_df = source_df,
                                                    checksum_df = checksum_df,
                                                    source_id_col = filename_col,
                                                    source_validation_col = args.verifier_col)
            if missing_imgs is not None:
                missing_imgs.to_csv(metadata_path + "_missing.csv", index = False)
                print(f"See {metadata_path}_missing.csv for missing image info and check logs.")
            else:
                print(f"Buddy check successful. All {expected_num_imgs} expected images accounted for.")
        except Exception as e:
            print(f"Verification of download failed due to {type(e).__name__}: {e}.")
            print("'BuddyCheck.validate_download' can be run directly on DataFrames of the source and checksum CSVs after correcting for this error.")
    return


if __name__ == "__main__":
    main()
