#CSV validation and input handling functions.

import sys


def validate_csv_extension(csv_path):
    """Validate that the input file has a .csv extension."""
    if not csv_path.endswith(".csv"):
        sys.exit("Expected CSV for input file; extension should be '.csv'")


def validate_filename_uniqueness(data_df, filename_col):
    """Validate that the filename column contains unique values."""
    if data_df.loc[data_df[filename_col].notna()].shape[0] != data_df[filename_col].nunique():
        sys.exit(f"{filename_col} is not a unique identifier for this dataset, please choose a column with unique values for filenames.")


def handle_missing_filenames(data_df, filename_col, url_col):
    """Handle cases where URLs exist but filenames are missing."""
    urls_no_name = len(data_df.loc[(data_df[filename_col].isna() & (data_df[url_col].notna()))])
    if urls_no_name > 0:
        ignore = input(f"'{filename_col}' is missing values for {urls_no_name} URLs. Proceed with download ignoring these URLs? [y/n]: ")
        if ignore.lower() != "y":
            sys.exit("Exited without executing.")


def setup_expected_columns(args):
    """Set up the expected columns dictionary for CSV processing."""
    subfolders = args.subdir_col
    expected_cols = {
        "filename_col": args.img_name_col.lower(),
        "url_col": args.url_col.lower()
    }
    if subfolders:
        subfolders = subfolders.lower()
        expected_cols["subfolders"] = subfolders
    return expected_cols, subfolders
