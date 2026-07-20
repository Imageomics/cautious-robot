# Downloads images from CSV with option to save downsampled copies.
# Logs image downloads and failures in json files.
# Logs response codes as strings, not int64.

# Logs saved in same folder as CSV used for download
# Downsized images are saved in <img_dir>_downsized

import pandas as pd
import argparse
import hashlib
import sys
from sumbuddy import get_checksums
from cautiousrobot.utils import process_csv
from cautiousrobot.buddy_check import BuddyCheck
from cautiousrobot.download import download_images
from cautiousrobot.roll_call import RollCall
from cautiousrobot.__about__ import __version__

def parse_args():
    available_algorithms = ', '.join(hashlib.algorithms_available)

    parser = argparse.ArgumentParser()
    # Use argument groups for required vs optional (both get short flags too) https://bugs.python.org/issue9694#msg132327
    # Optional arguments
    parser.add_argument(
    "--version",
    action="version",
    version=f"%(prog)s {__version__}",
    help="Show version number and exit",
)
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
    opt_args.add_argument("-a", "--checksum-algorithm", default = 'md5', #choices = available_algorithms,
                        help = f"checksum algorithm to use on images (default: md5, available: {available_algorithms})"
                        )
    opt_args.add_argument("-v", "--verifier-col", required = False, help = "name of column in source CSV with checksums (same hash as -a) to verify download", nargs = "?")
    
    return parser.parse_args()


def setup_log_paths(csv_path):
    """Set up the log file paths based on the CSV path."""
    metadata_path = csv_path.split(".")[0]
    log_filepath = metadata_path + "_log.jsonl"
    error_log_filepath = metadata_path + "_error_log.jsonl"
    return log_filepath, error_log_filepath, metadata_path


def process_checksums(img_dir, metadata_path, args, source_df):
    """Process checksums for downloaded images and verify if requested."""
    checksum_path = metadata_path + "_checksums.csv"
    try:
        get_checksums(input_path=img_dir, output_filepath=checksum_path, algorithm=args.checksum_algorithm)
        
        # Verify numbers
        checksum_df = pd.read_csv(checksum_path, low_memory=False)
        expected_num_imgs = source_df.shape[0]
        print(f"There are {checksum_df.shape[0]} files in {img_dir}. Based on {args.input_file}, there should be {expected_num_imgs} images.")
        
        return checksum_df, expected_num_imgs
    except Exception as e:
        print(f"checksum calculation of downloaded images was unsuccessful due to {e}.")
        print(f"you can get checksums for the images downloaded to {img_dir} by running sum-buddy directly.")
        return None, None


def verify_downloads(args, source_df, checksum_df, filename_col, metadata_path, expected_num_imgs):
    """Verify downloads using buddy check if verifier column is provided."""
    if not args.verifier_col:
        return
    
    # Run download verification
    buddy_check = BuddyCheck(buddy_id="filename", buddy_col=args.checksum_algorithm)
    try:
        missing_imgs = buddy_check.validate_download(
            source_df=source_df,
            checksum_df=checksum_df,
            source_id_col=filename_col,
            source_validation_col=args.verifier_col
        )
        if missing_imgs is not None:
            missing_imgs.to_csv(metadata_path + "_missing.csv", index=False)
            print(f"See {metadata_path}_missing.csv for missing image info and check logs.")
        else:
            print(f"Buddy check successful. All {expected_num_imgs} expected images accounted for.")
    except Exception as e:
        print(f"Verification of download failed due to {type(e).__name__}: {e}.")
        print("'BuddyCheck.validate_download' can be run directly on DataFrames of the source and checksum CSVs after correcting for this error.")


def main():
    args = parse_args()
    csv_path = args.input_file
    
    roll_call = RollCall(csv_path)

    # Validate CSV extension
    roll_call.validate_csv_extension(csv_path)

    # Set up expected columns and process CSV
    expected_cols, subfolders = roll_call.setup_expected_columns(args)
    try:
        data_df = process_csv(csv_path, expected_cols)
    except Exception as missing_cols:
        sys.exit(f"{missing_cols} Please adjust inputs and try again.")

    # Validate data and handle missing filenames
    filename_col = expected_cols["filename_col"]
    url_col = expected_cols["url_col"]
    roll_call.validate_filename_uniqueness(data_df, filename_col)
    roll_call.handle_missing_filenames(data_df, filename_col, url_col)
    
    # Set source DataFrame for only non-null filename values
    source_df = data_df.loc[data_df[filename_col].notna()].copy()

    # If a verifier column is provided, check for duplicate checksums before downloading
    if args.verifier_col:
        roll_call.check_duplicate_checksums(
            data_df=source_df,
            hash_col=args.verifier_col.lower(),
            ignore_duplicates=getattr(args, "ignore_duplicates", False)
        )

    # Validate and handle existing output directory
    img_dir = args.output_dir
    source_df, filtered_df = roll_call.check_existing_images(csv_path, img_dir, source_df, filename_col, subfolders)

    # Prepare summary info
    num_images = filtered_df.shape[0]
    downsample_dir = img_dir + "_downsized" if isinstance(args.side_length, int) else None

    # Print RollCall summary
    roll_call.print_download_summary(
        img_dir=img_dir,
        downsample_dir=downsample_dir,
        subfolders=subfolders,
        num_images=num_images
    )

    # Set up log paths
    log_filepath, error_log_filepath, metadata_path = setup_log_paths(csv_path)

    # Download images with or without downsampling
    if downsample_dir:
        # Download images from urls & save downsample copy
        download_images(filtered_df,
                       img_dir=img_dir,
                       log_filepath=log_filepath,
                       error_log_filepath=error_log_filepath,
                       filename=filename_col,
                       subfolders=subfolders,
                       downsample_path=downsample_dir,
                       downsample=args.side_length,
                       file_url=url_col,
                       wait=args.wait_time,
                       retry=args.max_retries)
        print(f"Images downloaded from {csv_path} to {img_dir}, with downsampled images in {downsample_dir}.")
    else:
        # Download images from urls without downsample copy
        download_images(filtered_df,
                       img_dir=img_dir,
                       log_filepath=log_filepath,
                       error_log_filepath=error_log_filepath,
                       filename=filename_col,
                       subfolders=subfolders,
                       file_url=url_col,
                       wait=args.wait_time,
                       retry=args.max_retries)
        print(f"Images downloaded from {csv_path} to {img_dir}.")
    
    print(f"Download logs are in {log_filepath} and {error_log_filepath}.")

    # Process checksums and verify downloads
    checksum_df, expected_num_imgs = process_checksums(img_dir, metadata_path, args, source_df)
    if checksum_df is not None:
        verify_downloads(args, source_df, checksum_df, filename_col, metadata_path, expected_num_imgs)


if __name__ == "__main__":
    main()
