"""
Checksum verification and download validation module.

This module provides functions for generating checksums for downloaded images
and verifying that downloads match expected checksums using the BuddyCheck class.
"""

import pandas as pd
from sumbuddy import get_checksums
from cautiousrobot.buddy_check import BuddyCheck


def process_checksums(img_dir, metadata_path, args, source_df):
    """
    Process checksums for downloaded images.

    Generates checksums for all files in the image directory using the specified
    algorithm and compares the count of downloaded images against expected count.

    Args:
        img_dir (str): Path to the directory containing downloaded images.
        metadata_path (str): Base path for saving the checksums CSV file.
        args: Arguments object containing:
            - checksum_algorithm (str): Hash algorithm to use (e.g., 'md5', 'sha256')
            - input_file (str): Path to the input CSV file
        source_df (pd.DataFrame): DataFrame containing source image information.

    Returns:
        tuple: (checksum_df, expected_num_imgs) where:
            - checksum_df (pd.DataFrame or None): DataFrame with checksums if successful,
              None if checksum calculation failed.
            - expected_num_imgs (int or None): Expected number of images based on source_df,
              None if checksum calculation failed.

    Raises:
        Prints error messages to console if checksum calculation fails.
    """
    checksum_path = metadata_path + "_checksums.csv"
    try:
        get_checksums(
            input_path=img_dir,
            output_filepath=checksum_path,
            algorithm=args.checksum_algorithm
        )

        # Verify numbers
        checksum_df = pd.read_csv(checksum_path, low_memory=False)
        expected_num_imgs = source_df.shape[0]
        print(
            f"There are {checksum_df.shape[0]} files in {img_dir}. "
            f"Based on {args.input_file}, there should be {expected_num_imgs} images."
        )

        return checksum_df, expected_num_imgs
    except Exception as e:
        print(
            f"checksum calculation of downloaded images was unsuccessful due to {e}."
        )
        print(
            f"you can get checksums for the images downloaded to {img_dir} "
            f"by running sum-buddy directly."
        )
        return None, None


def verify_downloads(
    args, source_df, checksum_df, filename_col, metadata_path, expected_num_imgs
):
    """
    Verify downloaded images against expected checksums.

    Uses BuddyCheck to validate that downloaded images match expected checksums
    from the source CSV. Only runs if a verifier column is specified.

    Args:
        args: Arguments object containing:
            - verifier_col (str or None): Column name in source CSV with expected checksums
            - checksum_algorithm (str): Hash algorithm name used for checksums
        source_df (pd.DataFrame): DataFrame containing source image information.
        checksum_df (pd.DataFrame): DataFrame with calculated checksums from downloaded images.
        filename_col (str): Name of the column containing image filenames.
        metadata_path (str): Base path for saving the missing images CSV file.
        expected_num_imgs (int): Expected number of images.

    Returns:
        None: Prints verification results to console.

    Raises:
        Prints error messages to console if verification fails.
    """
    if not args.verifier_col:
        return

    # Run download verification
    buddy_check = BuddyCheck(buddy_id="filename", buddy_col=args.checksum_algorithm)
    try:
        missing_imgs = buddy_check.validate_download(
            source_df=source_df,
            checksum_df=checksum_df,
            source_id_col=filename_col,
            source_validation_col=args.verifier_col,
        )
        if missing_imgs is not None:
            missing_imgs.to_csv(metadata_path + "_missing.csv", index=False)
            print(f"See {metadata_path}_missing.csv for missing image info and check logs.")
        else:
            print(
                f"Buddy check successful. All {expected_num_imgs} expected images accounted for."
            )
    except Exception as e:
        print(f"Verification of download failed due to {type(e).__name__}: {e}.")
        print(
            "'BuddyCheck.validate_download' can be run directly on DataFrames "
            "of the source and checksum CSVs after correcting for this error."
        )
