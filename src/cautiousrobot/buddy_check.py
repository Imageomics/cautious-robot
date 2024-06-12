import sys
import pandas as pd
from cautiousrobot.utils import process_csv


class BuddyCheck:
    def __init__(self, buddy_id = None, buddy_col = "md5"):
        self.buddy_id = buddy_id
        self.buddy_col = buddy_col


    def check_alignment(self, img_df, checksum_df, id_col = "filename", validation_col = "checksum", buddy_id = None, buddy_col = None):
        '''
        Check that all expected images were downloaded.
        Merges on the filename and checksum columns for both the source file and the checksum file produced by sum-buddy.
        If buddy_id is not given, merges on just the checksum columns--not recommended if duplicate images are possible.
        Saves a missing images CSV if there are less than the expected number of matches and prints the number missing.

        Parameters:
        img_df - DataFrame with unique filenames and expected checksums.
        checksum_df - DataFrame with checksums of images listed in source_filepath.
        id_col - String. Name of unique identifier column for source CSV. Default: 'filename'.
        validation_col - String. Name of column in source CSV with expected checksums. Default: 'checksum'.
        buddy_id - String [optional]. Name of matching unique identifier column for checksum CSV. Default: 'filename'.
        buddy_col - String. Column name for checksums in checksum CSV (algorithm used for the checksums with sum-buddy). Default: 'md5'.
        
        '''
        if buddy_col is None:
            buddy_col = self.buddy_col
        if buddy_id is None:
            buddy_id = self.buddy_id

        df = img_df.loc[img_df[id_col].notna()].copy()

        if buddy_id is None:
            # merge on checksums
            dl_match = pd.merge(df,
                                checksum_df,
                                left_on = validation_col,
                                right_on = buddy_col,
                                how = "inner")
        else:
            # merge on checksums & IDs
            dl_match = pd.merge(df,
                                checksum_df,
                                left_on = [id_col, validation_col],
                                right_on = [buddy_id, buddy_col],
                                how = "inner")

        # id_col should be non-null
        if dl_match.shape[0] < df.shape[0]:
            downloaded_ids = list(dl_match[id_col].unique())
            missing_imgs = df.loc[~df[id_col].isin(downloaded_ids)].copy()
            return missing_imgs
        return None

    def validate_download(self, img_source, checksum_source, id_col = "filename", validation_col = "checksum", buddy_id = None, buddy_col = None):
        '''
        Check that all expected images were downloaded.
        Merges on the filename and checksum columns for both the source file and the checksum file produced by sum-buddy.
        If buddy_id is not given, merges on just the checksum columns--not recommended if duplicate images are possible.
        Saves a missing images CSV if there are less than the expected number of matches and prints the number missing.

        Parameters:
        img_source - DataFrame (or path to CSV file) with unique filenames and expected checksums.
        checksum_source - DataFrame (or path to CSV file) with checksums of images listed in source_filepath.
        id_col - String. Name of unique identifier column for source CSV. Default: 'filename'.
        validation_col - String. Name of column in source CSV with expected checksums. Default: 'checksum'.
        buddy_id - String [optional]. Name of matching unique identifier column for checksum CSV. Default: 'filename'.
        buddy_col - String. Column name for checksums in checksum CSV (algorithm used for the checksums with sum-buddy). Default: 'md5'.
        
        '''
        if buddy_col is None:
            buddy_col = self.buddy_col
        else:
            buddy_col = buddy_col.lower()

        # Process source & checksum files
        expected_cols = {"id_col": id_col.lower(), "checksum_col": validation_col.lower()}
        try:
            df = process_csv(img_source, expected_cols)
        except Exception as missing_cols:
            sys.exit(f"{missing_cols} Please adjust inputs and try again.")
        df_checksum = process_csv(checksum_source, {"checksum_col": buddy_col})
        
        missing_imgs = self.check_alignment(img_df = df,
                                   checksum_df = df_checksum,
                                   id_col = expected_cols["id_col"],
                                   validation_col = expected_cols["checksum_col"],
                                   buddy_id = buddy_id,
                                   buddy_col = buddy_col
                                   )
        
        if missing_imgs is not None:
            # Set location for missing img record
            metadata_path = img_source.split(".")[0]
            missing_imgs.to_csv(metadata_path + "_missing.csv", index = False)
            print(f"Image mismatch: {missing_imgs.shape[0]} image(s) not aligned, see {metadata_path}_missing.csv for missing image info")
