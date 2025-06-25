# Download functionality for cautious-robot
# Contains helper functions for downloading images from CSV data

import requests
import shutil
import os
import time
from tqdm import tqdm
from cautiousrobot.utils import log_response, update_log, downsample_and_save_image

# Constants
REDO_CODE_LIST = [429, 500, 502, 503, 504]


def create_image_directory(image_dir_path):
    """
    Create the directory for storing images if it doesn't exist.
    
    Parameters:
    - image_dir_path (str): Path to the directory where images should be stored
    
    Returns:
    - None
    """
    if not os.path.exists(image_dir_path):
        os.makedirs(image_dir_path, exist_ok=False)


def get_image_path(img_dir, subfolders, data, i, filename):
    """
    Determine the full path for an image based on subfolder configuration.
    
    Parameters:
    - img_dir (str): Base directory for images
    - subfolders (str): Column name for subfolder organization (can be None)
    - data (DataFrame): DataFrame containing image data
    - i (int): Current row index
    - filename (str): Column name for filename
    
    Returns:
    - tuple: (image_dir_path, image_name)
    """
    image_dir_path = img_dir
    image_name = data[filename][i]
    
    if subfolders:
        image_dir_path = img_dir + "/" + data[subfolders][i]
    
    return image_dir_path, image_name


def handle_missing_url(log_errors, i, image_name, url, error_log_filepath):
    """
    Handle cases where the URL is missing or empty.
    
    Parameters:
    - log_errors (dict): Dictionary to store error logs
    - i (int): Current row index
    - image_name (str): Name of the image
    - url (str): URL that is missing
    - error_log_filepath (str): Path to error log file
    
    Returns:
    - dict: Updated log_errors dictionary
    """
    log_errors = log_response(log_errors,
                            index=i,
                            image=image_name,
                            file_path=url,
                            response_code="no url")
    update_log(log=log_errors, index=i, filepath=error_log_filepath)
    return log_errors


def download_single_image(url, image_name, image_dir_path, log_data, log_errors, 
                         log_filepath, error_log_filepath, i, wait, retry):
    """
    Download a single image with retry logic and error handling.
    
    Parameters:
    - url (str): URL to download the image from
    - image_name (str): Name to save the image as
    - image_dir_path (str): Directory path to save the image
    - log_data (dict): Dictionary to store successful download logs
    - log_errors (dict): Dictionary to store error logs
    - log_filepath (str): Path to success log file
    - error_log_filepath (str): Path to error log file
    - i (int): Current row index
    - wait (int): Seconds to wait between retries
    - retry (int): Maximum number of retries
    
    Returns:
    - tuple: (log_data, log_errors, success)
    """
    redo = True
    max_redos = retry
    
    while redo and max_redos > 0:
        try:
            response = requests.get(url, stream=True)
        except Exception as e:
            redo = True
            max_redos -= 1
            if max_redos <= 0:
                log_errors = log_response(log_errors,
                                        index=i,
                                        image=image_name,
                                        file_path=url,
                                        response_code=str(e))
                update_log(log=log_errors, index=i, filepath=error_log_filepath)
            continue
        
        if response.status_code == 200:
            redo = False
            # Log successful download
            log_data = log_response(log_data,
                                  index=i,
                                  image=image_name,
                                  file_path=url,
                                  response_code=response.status_code)
            update_log(log=log_data, index=i, filepath=log_filepath)
            
            # Create directory and save image
            create_image_directory(image_dir_path)
            with open(f"{image_dir_path}/{image_name}", "wb") as out_file:
                shutil.copyfileobj(response.raw, out_file)
            
            del response
            return log_data, log_errors, True
            
        elif response.status_code in REDO_CODE_LIST:
            redo = True
            max_redos -= 1
            if max_redos <= 0:
                log_errors = log_response(log_errors,
                                        index=i,
                                        image=image_name,
                                        file_path=url,
                                        response_code=response.status_code)
                update_log(log=log_errors, index=i, filepath=error_log_filepath)
            else:
                time.sleep(wait)
        else:  # Other failures (e.g., 404)
            redo = False
            log_errors = log_response(log_errors,
                                    index=i,
                                    image=image_name,
                                    file_path=url,
                                    response_code=response.status_code)
            update_log(log=log_errors, index=i, filepath=error_log_filepath)
        
        del response
    
    return log_data, log_errors, False


def process_downsampling(data, i, image_name, image_dir_path, downsample_path, 
                        subfolders, downsample, log_errors, url, error_log_filepath):
    """
    Handle downsampling of an image if requested.
    
    Parameters:
    - data (DataFrame): DataFrame containing image data
    - i (int): Current row index
    - image_name (str): Name of the image
    - image_dir_path (str): Directory path of the original image
    - downsample_path (str): Base path for downsampled images
    - subfolders (str): Column name for subfolder organization
    - downsample (int): Size for downsampled images
    - log_errors (dict): Dictionary to store error logs
    - url (str): URL of the image
    - error_log_filepath (str): Path to error log file
    
    Returns:
    - dict: Updated log_errors dictionary
    """
    if not downsample:
        return log_errors
    
    downsample_dir_path = downsample_path
    if subfolders:
        downsample_dir_path = downsample_path + "/" + data[subfolders][i]
    
    if os.path.exists(downsample_dir_path + "/" + image_name):
        # Don't overwrite resized images
        return log_errors
    
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
    
    return log_errors


def download_images(data, img_dir, log_filepath, error_log_filepath, filename="filename",
                   subfolders=None, downsample_path=None, downsample=None,
                   file_url="file_url", wait=3, retry=5, starting_index=0):
    """
    Download images to img_dir and downsampled images to a chosen downsized image path.

    Parameters:
    - data (DataFrame): DataFrame with image metadata (must have filename and file_url)
    - img_dir (str): Directory in which to save images
    - log_filepath (str): Filepath for the download log
    - error_log_filepath (str): Filepath for the download error log
    - filename (str): Name of column to use for image filenames (default: 'filename')
    - subfolders (str): Name of column to use for subfolder designations (optional)
    - downsample_path (str): Folder to which to download the downsized images (optional)
    - downsample (int): Number of pixels per side for downsampled images (optional)
    - file_url (str): Name of column to use for image urls (default: 'file_url')
    - wait (int): Seconds to wait between retries for an image (default: 3)
    - retry (int): Max number of times to retry downloading an image (default: 5)
    - starting_index (int): Index at which to start the download (default: 0)
    
    Returns:
    - None
    """
    log_data = {}
    log_errors = {}

    # Process each row in the DataFrame
    for i in tqdm(data.index):
        if i < starting_index:
            continue
            
        # Get image path and name
        image_dir_path, image_name = get_image_path(img_dir, subfolders, data, i, filename)
        
        # Skip if image already exists
        if os.path.exists(image_dir_path + "/" + image_name):
            continue
        
        # Get URL and handle missing URLs
        url = data[file_url][i]
        if not url:
            log_errors = handle_missing_url(log_errors, i, image_name, url, error_log_filepath)
            continue
        
        # Download the image
        log_data, log_errors, success = download_single_image(
            url, image_name, image_dir_path, log_data, log_errors,
            log_filepath, error_log_filepath, i, wait, retry
        )
        
        # Handle downsampling if requested
        if success:
            log_errors = process_downsampling(
                data, i, image_name, image_dir_path, downsample_path,
                subfolders, downsample, log_errors, url, error_log_filepath
            ) 