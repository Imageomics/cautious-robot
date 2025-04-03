import pandas as pd
from cautiousrobot.exceptions import EmptyDataFrameError


class BuddyCheck:
    def __init__(self, buddy_id = None, buddy_col = "md5"):
        '''
        Parameters:
        -----------
        buddy_id - String [optional]. Name of matching unique identifier column for checksum_df. Matches on both filename and checksum values when provided.
        buddy_col - String. Column name for checksums in checksum_df (algorithm used for the checksums with sum-buddy). Default: 'md5'.
        '''
        self.buddy_id = buddy_id
        self.buddy_col = buddy_col


    def merge_on_checksum(self, source_df, checksum_df, source_validation_col):
        '''
        Merge source and checksum DataFrames on only checksum values.
        '''
        print("merging on checksums only")
        merged_df = pd.merge(source_df,
                             checksum_df,
                             left_on = source_validation_col,
                             right_on = self.buddy_col,
                             how = "inner")
        return merged_df
    
    
    def merge_on_filename_checksum(self, source_df, checksum_df, source_id_col, source_validation_col):
        '''
        Merge source and checksum DataFrames on both filename and checksum values.
        '''
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

        if source_df.empty:
            raise EmptyDataFrameError("source_df")
        if checksum_df.empty:
            raise EmptyDataFrameError("checksum_df")

        if self.buddy_id is None:
            check_type = "checksums"
            merged_df = self.merge_on_checksum(source_df, checksum_df, source_validation_col)
            missing_imgs = self.check_alignment(source_df, merged_df, source_id_col)
        else:
            check_type = "checksums and filenames"
            merged_df = self.merge_on_filename_checksum(source_df, checksum_df, source_id_col, source_validation_col)
            missing_imgs = self.check_alignment(source_df, merged_df, source_id_col)

        if missing_imgs is not None:
            print(f"Image mismatch: {missing_imgs.shape[0]} image(s) not aligned after merging on {check_type}.")
        return missing_imgs
