# Download functionality for cautious-robot
# Contains helper functions for downloading images from CSV data

import requests
import shutil
import os
import time
import mimetypes
import pandas as pd
from urllib.parse import urlparse
from tqdm import tqdm
from cautiousrobot.utils import log_response, update_log, downsample_and_save_image

# Constants
# 403 (Forbidden) is included here because some APIs appear use it to signal rate limiting.
# In such cases, retrying with exponential backoff might succeed once the rate limit resets.
REDO_CODE_LIST = [403, 429, 500, 502, 503, 504]



def extract_extension_from_filename(filename):
    """
    Extract file extension from filename/path using os.path.splitext.

    Parameters:
    - filename (str): The filename or path

    Returns:
    - str or None: The file extension (with dot) or None if no extension
    """
    _, ext = os.path.splitext(filename)
    return ext if ext else None


def extract_extension_from_url(url):
    """
    Extract file extension from URL path using urllib.parse and os.path.splitext.
    Ignores query parameters and fragments.

    Parameters:
    - url (str): The URL

    Returns:
    - str or None: The file extension (with dot) or None if no extension
    """
    parsed = urlparse(url)
    _, ext = os.path.splitext(parsed.path)
    return ext if ext else None


def get_content_type_from_url(url):
    """
    Get Content-Type from URL using HTTP HEAD request.

    Parameters:
    - url (str): The URL to check

    Returns:
    - str or None: The Content-Type (main type only, without parameters) or None if unavailable
    """
    if is_url_missing_or_invalid(url):
        return None

    try:
        response = requests.head(url, timeout=10)
        content_type = response.headers.get('content-type', '')
        # Split off parameters like '; charset=utf-8'
        return content_type.split(';')[0].strip() if content_type else None
    except Exception:
        return None


def are_extensions_equivalent(ext1, ext2, url):
    """
    Check if two extensions are effectively equivalent by comparing their MIME types
    and the actual Content-Type served by the URL.

    Parameters:
    - ext1 (str): First extension (with dot)
    - ext2 (str): Second extension (with dot)
    - url (str): URL to check actual Content-Type

    Returns:
    - bool: True if extensions are equivalent, False otherwise
    """
    # Quick check - if normalized extensions are identical, they're equivalent
    if ext1.lower() == ext2.lower():
        return True

    # Get MIME types for both extensions
    mime1 = mimetypes.guess_type(f"file{ext1}")[0]
    mime2 = mimetypes.guess_type(f"file{ext2}")[0]

    # If MIME types are different, they're not equivalent
    if mime1 != mime2 or not mime1:
        return False

    # Get actual Content-Type from server to verify
    actual_content_type = get_content_type_from_url(url)

    # All should match for true equivalency
    return mime1 == actual_content_type


def resolve_filename_with_extension(base_filename, url):
    """
    Resolve the final filename with proper extension by checking both
    the filename and URL, handling conflicts, and inferring from HTTP headers if needed.

    Parameters:
    - base_filename (str): The base filename from the image name column
    - url (str): The URL to download from (can be None/empty)

    Returns:
    - tuple: (filename_or_None, error_message_or_None)
             If successful: (resolved_filename, None)
             If conflict: (None, error_message)
    """
    name_ext = extract_extension_from_filename(base_filename)

    # If URL is missing/invalid, just return the base filename
    if is_url_missing_or_invalid(url):
        return base_filename, None

    url_ext = extract_extension_from_url(url)

    # Both have extensions - check for conflicts
    if name_ext and url_ext:
        if are_extensions_equivalent(name_ext, url_ext, url):
            # Use name column extension (user preference)
            return base_filename, None
        else:
            error_msg = (
                f"Mismatching extensions in input data may cause unexpected behavior: "
                f"filename has '{name_ext}' but URL suggests '{url_ext}'. "
                f"These extensions are not equivalent for content served by {url}"
            )
            return None, error_msg

    # Only name has extension
    if name_ext:
        return base_filename, None

    # Only URL has extension
    if url_ext:
        return base_filename + url_ext, None

    # Neither has extension - try to infer from Content-Type
    content_type = get_content_type_from_url(url)
    if content_type:
        inferred_ext = mimetypes.guess_extension(content_type)
        if inferred_ext:
            return base_filename + inferred_ext, None

    # No extension can be determined
    return base_filename, None


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


def get_image_path(img_dir, subfolders, data, i, filename, file_url):
    """
    Determine the full path for an image based on subfolder configuration.
    Resolves the final filename with proper extension.

    Parameters:
    - img_dir (str): Base directory for images
    - subfolders (str): Column name for subfolder organization (can be None)
    - data (DataFrame): DataFrame containing image data
    - i (int): Current row index
    - filename (str): Column name for filename
    - file_url (str): Column name for file URL

    Returns:
    - tuple: (image_dir_path, image_name, error_message)
             If successful: (path, name, None)
             If extension conflict: (None, None, error_message)
    """
    image_dir_path = img_dir
    base_filename = str(data[filename][i])
    url = data[file_url][i]

    # Resolve filename with proper extension
    image_name, error_msg = resolve_filename_with_extension(base_filename, url)

    if error_msg:
        return None, None, error_msg

    if subfolders:
        image_dir_path = img_dir + "/" + str(data[subfolders][i])

    return image_dir_path, image_name, None


def is_url_missing_or_invalid(url):
    """
    Check if a URL is missing or invalid.

    Parameters:
    - url: The URL value to check

    Returns:
    - bool: True if URL is missing/invalid, False otherwise
    """
    if pd.isna(url):
        return True
    if not url:
        return True
    if str(url).lower().strip() in ['nan', 'none', 'null', '']:
        return True
    return False


def handle_download_skip(log_errors, i, image_name, url, error_code, error_log_filepath):
    """
    Handle cases where download must be skipped due to data issues.

    Parameters:
    - log_errors (dict): Dictionary to store error logs
    - i (int): Current row index
    - image_name (str): Name of the image
    - url (str): URL from the CSV
    - error_code (str): Specific error code ("invalid url", "extension mismatch", etc.)
    - error_log_filepath (str): Path to error log file

    Returns:
    - dict: Updated log_errors dictionary
    """
    log_errors = log_response(log_errors,
                            index=i,
                            image=image_name,
                            file_path=str(url) if url else "N/A",
                            response_code=error_code)
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
    attempts_remaining = retry

    while redo and attempts_remaining > 0:
        try:
            response = requests.get(url, stream=True)
        except Exception as e:
            redo = True
            attempts_remaining -= 1
            if attempts_remaining <= 0:
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
            attempts_remaining -= 1
            if attempts_remaining <= 0:
                log_errors = log_response(log_errors,
                                        index=i,
                                        image=image_name,
                                        file_path=url,
                                        response_code=response.status_code)
                update_log(log=log_errors, index=i, filepath=error_log_filepath)
            else:
                # Exponential backoff: longer waits for 403 (rate limiting)
                if response.status_code == 403:
                    backoff_wait = wait * (2 ** (retry - attempts_remaining))  # 2x, 4x, 8x, etc.
                    time.sleep(backoff_wait)
                else:
                    time.sleep(wait)
                del response
                continue
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
                   file_url="file_url", wait=3, retry=5):
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
    
    Returns:
    - None
    """
    log_data = {}
    log_errors = {}

    # Process each row in the DataFrame
    for i in tqdm(data.index):

        # Get URL and handle missing URLs first (before any path operations)
        url = data[file_url][i]
        if is_url_missing_or_invalid(url):
            # Use a basic filename for logging since we can't resolve the full path yet
            base_filename = str(data[filename][i])
            log_errors = handle_download_skip(log_errors, i, base_filename, url, "invalid url", error_log_filepath)
            continue

        # Get image path and name (now safe since URL is validated)
        image_dir_path, image_name, extension_error = get_image_path(img_dir, subfolders, data, i, filename, file_url)

        # Handle extension mismatch
        if extension_error:
            base_filename = str(data[filename][i])
            log_errors = handle_download_skip(log_errors, i, base_filename, url, "extension mismatch", error_log_filepath)
            continue

        # Skip if image already exists
        if os.path.exists(image_dir_path + "/" + image_name):
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