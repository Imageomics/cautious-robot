# Image downsampling and processing

import os
from PIL import Image
from cautiousrobot.utils import log_response, update_log


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
