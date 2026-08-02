class EmptyDataFrameError(Exception):
    def __init__(self, df_name):
        message = f"Input DataFrame {df_name} is empty."
        super().__init__(message)

class MissingColumnsError(Exception):
    def __init__(self, csv_path, column_names, expected_columns):
        message = f"The CSV at {csv_path} is missing column(s): {column_names}, defined as {expected_columns}."
        super().__init__(message)

class ChecksumError(Exception):
    def __init__(self, error, img_dir):
        self.error = error
        self.img_dir = img_dir
        message = f"Checksum calculation of downloaded images was unsuccessful due to {self.error}.\n You can get checksums for the images downloaded to {self.img_dir} by running sum-buddy directly."
        super().__init__(message)
        
class BuddyCheckError(Exception):
    def __init__(self, error, reason):
        self.error = error
        self.reason = reason
        message = f"Verification of download failed due to {self.error}: {self.reason}.\n'BuddyCheck.validate_download' can be run directly on DataFrames of the source and checksum CSVs after correcting for this error."
        super().__init__(message)
        