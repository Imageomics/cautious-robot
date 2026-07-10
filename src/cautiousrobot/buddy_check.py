import pandas as pd
from cautiousrobot.exceptions import EmptyDataFrameError
from sumbuddy import get_checksums


class BuddyCheck:
    """Validate downloaded files against expected checksums and identifiers."""

    def __init__(self, buddy_id = None, buddy_col = "md5"):
        """
        Initialize the checksum validator.

        Parameters:
            -----------
        buddy_id - String [optional]. Name of matching unique identifier column for checksum_df. Matches on both filename and checksum values when provided.
        buddy_col - String. Column name for checksums in checksum_df (algorithm used for the checksums with sum-buddy). Default: 'md5'.
        """
        self.buddy_id = buddy_id
        self.buddy_col = buddy_col


    def merge_on_checksum(self, source_df, checksum_df, source_validation_col):
        """
        Merge the source and checksum DataFrames on only checksum values.

        Parameters:
        -----------
            source_df (pd.DataFrame): DataFrame containing the expected image metadata, must include a validation column.
            checksum_df (pd.DataFrame): DataFrame containing the checksums of downloaded images.
            source_validation_col (str): Name of the checksum column in the source DataFrame.

        Returns:
            merged_df (pd.DataFrame): DataFrame of only entries that matched on checksum values alone.
        """
        # Use checksum-only matching when no identifier column is available.
        print("merging on checksums only")
        merged_df = pd.merge(source_df,
                             checksum_df,
                             left_on = source_validation_col,
                             right_on = self.buddy_col,
                             how = "inner")
        return merged_df
    
    
    def merge_on_filename_checksum(self, source_df, checksum_df, source_id_col, source_validation_col):
        """
        Merge the source and checksum DataFrames using both filename and checksum values.

        Parameters:
        -----------
            source_df (pd.DataFrame): DataFrame containing the expected image metadata, must include a validation column.
            checksum_df (pd.DataFrame): DataFrame containing the checksums of downloaded images.
            source_id_col (str): Name of the identifier column in the source DataFrame; containing the unique image filenames.
            source_validation_col (str): Name of the checksum column in the source DataFrame.

        Returns:
            merged_df (pd.DataFrame): DataFrame of only entries that matched on both identifier and checksum values.
        """
        print("merging on checksums and IDs")
        merged_df = pd.merge(source_df,
                             checksum_df,
                             left_on = [source_id_col, source_validation_col],
                             right_on = [self.buddy_id, self.buddy_col],
                             how = "inner")
        return merged_df
    

    def check_alignment(self, source_df, merged_df, id_col = "filename"):
        '''
        Check that all expected images were downloaded and record those that aren't with full source_df information.
    
        Parameters:
        source_df - DataFrame with unique filenames and expected checksums.
        merged_df - DataFrame from inner merge of source_df and checksum_df (record of all downloaded images).
        id_col - String. Name of unique identifier column for source_df. Number of non-null values must match expected number of images. Default: 'filename'.
        
        Returns:
        missing_imgs - DataFrame. Subset of img_df that didn't match checksum_df, None if all match.
        '''
        # If fewer rows matched than expected, the missing items are those not present in the merged result.
        if merged_df.shape[0] < source_df.shape[0]:
            downloaded_ids = list(merged_df[id_col].unique())
            missing_imgs = source_df.loc[~source_df[id_col].isin(downloaded_ids)].copy()
            return missing_imgs
        return None

    def validate_download(self, source_df, checksum_df, source_id_col = "filename", source_validation_col = "checksum"):
        '''
        Check that all expected images were downloaded.
        Merges on the filename and checksum columns for both the source file and the checksum file produced by sum-buddy.
        If buddy_id is not given, merges on just the checksum columns--not recommended if duplicate images are possible.
        Returns a DataFrame of missing images if there are less than the expected number of matches and prints the number missing.

        Parameters:
        source_df - DataFrame with unique filenames and expected checksums.
        checksum_source - DataFrame with checksums of images listed in source_df. Filename and checksum column names must match 'buddy_id' and 'buddy_col', respectively.
        source_id_col - String. Name of unique identifier column for source_df. Number of non-null values must match expected number of images. Default: 'filename'.
        source_validation_col - String. Name of column in source_df with expected checksums. Default: 'checksum'.
        
        Returns:
        missing_imgs - DataFrame. Subset of source_df that didn't match checksum_df, None if all match.        
        '''
        # Validate the inputs before running the merge-based checks.
        if source_df.empty:
            raise EmptyDataFrameError("source_df")
        if checksum_df.empty:
            raise EmptyDataFrameError("checksum_df")

        if self.buddy_id is None:
            # Fall back to checksum-only matching when no identifier is configured.
            check_type = "checksums"
            merged_df = self.merge_on_checksum(source_df, checksum_df, source_validation_col)
            missing_imgs = self.check_alignment(source_df, merged_df, source_id_col)
        else:
            # Use both the identifier and checksum to reduce false positives from duplicate hashes.
            check_type = "checksums and filenames"
            merged_df = self.merge_on_filename_checksum(source_df, checksum_df, source_id_col, source_validation_col)
            missing_imgs = self.check_alignment(source_df, merged_df, source_id_col)

        if missing_imgs is not None:
            # Report how many records failed to align after the merge.
            print(f"Image mismatch: {missing_imgs.shape[0]} image(s) not aligned after merging on {check_type}.")
        return missing_imgs


def process_checksums(img_dir, metadata_path, args, source_df):
    """
    Calls sum-buddy function to generate checksums for all downloaded files using the specified
    algorithm and compares the count of downloaded images against the expected number.

        Parameters:
        -----------
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

    Notes:
        Catches exceptions raised during checksum calculation and prints an error message with instructions to console and returns (None, None).
    """
    # Build the output path for the checksum CSV before running the checksum step.
    checksum_path = metadata_path + "_checksums.csv"
    try:
        # Generate checksums for the downloaded images using the requested algorithm.
        get_checksums(
            input_path=img_dir,
            output_filepath=checksum_path,
            algorithm=args.checksum_algorithm
        )

        # Compare number of downloaded images to the expected number of images.
        checksum_df = pd.read_csv(checksum_path, low_memory=False)
        expected_num_imgs = source_df.shape[0]
        print(
            f"There are {checksum_df.shape[0]} files in {img_dir}. "
            f"Based on {args.input_file}, there should be {expected_num_imgs} images."
        )

        return checksum_df, expected_num_imgs
    except Exception as e:
        # If checksum generation fails, report the error and return no results for downstream handling.
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
    Use BuddyCheck to verify the download integrity, if a verifier column is provided, and saves a CSV of missing images if the download is incomplete.

        Parameters:
        -----------
        args: Arguments object containing:
            - verifier_col (str or None): Column name in source CSV with expected checksums.
            - checksum_algorithm (str): Name of checksum algorithm used to calculate checksums in 'verifier_col'.
        source_df (pd.DataFrame): DataFrame containing source image information.
        checksum_df (pd.DataFrame): DataFrame with calculated checksums from downloaded images.
        filename_col (str): Name of the column containing image filenames.
        metadata_path (str): Base path for saving the missing images CSV file.
        checksum_df (pd.DataFrame): DataFrame with checksums of downloaded images, calculated with sum-buddy.
        filename_col (str): Name of the column, in the source DataFrame, containing image filenames.
        metadata_path (str): Base path for saving the missing images CSV file.
        expected_num_imgs (int): Number of images expected to have been downloaded.

    Returns:
        None: Prints verification results to console. Saves a CSV of missing images if download is incomplete.

    Notes:
        Catches exceptions raised during verification and prints an error message with instructions to console.
    """
    # Skip verification entirely when no verifier column was supplied.
    if not args.verifier_col:
        return

    # Create a validator that compares the expected source checksums with the generated checksum output.
    buddy_check = BuddyCheck(buddy_id="filename", buddy_col=args.checksum_algorithm)
    try:
        # Run the verification step using the requested filename column and validator column.
        missing_imgs = buddy_check.validate_download(
            source_df=source_df,
            checksum_df=checksum_df,
            source_id_col=filename_col,
            source_validation_col=args.verifier_col,
        )
        if missing_imgs is not None:
            # Save the records that failed validation for later review and debugging.
            missing_imgs.to_csv(metadata_path + "_missing.csv", index=False)
            print(f"See {metadata_path}_missing.csv for missing image info and check logs.")
        else:
            # No missing images
            print(
                f"Buddy check successful. All {expected_num_imgs} expected images accounted for."
            )
    except Exception as e:
        print(f"Verification of download failed due to {type(e).__name__}: {e}.")
        print(
            "'BuddyCheck.validate_download' can be run directly on DataFrames "
            "of the source and checksum CSVs after correcting for this error."
        )
