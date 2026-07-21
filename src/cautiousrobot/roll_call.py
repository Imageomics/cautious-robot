# CSV validation and input handling functions.

import os
import sys
from sumbuddy import gather_file_paths
from sumbuddy.exceptions import EmptyInputDirectoryError


class RollCall:
    """Pre-download validation and directory verification."""

    def __init__(self, csv_path=None):
        """
        Initialize the roll-call validator with an optional CSV path.

        Parameters:
            csv_path (str): Path to the source CSV file used for reporting and output naming.
        """
        self.csv_path = csv_path

    def validate_csv_extension(self, csv_path):
        """
        Validate that the supplied input path points to a CSV file.

        Parameters:
            csv_path (str): Path to the input CSV file.

        Raises:
            SystemExit: If the input file does not end with the '.csv' extension.
        """
        # The CLI expects CSV input, so the validation stops early when the file type is wrong.
        if not csv_path.lower().endswith(".csv"):
            sys.exit("Expected CSV for input file; extension should be '.csv'")

    def validate_filename_uniqueness(self, data_df, filename_col):
        """
        Validate that the selected filename column contains unique values.

        Parameters:
            data_df (pd.DataFrame): DataFrame loaded from the CSV file.
            filename_col (str): Name of the column that should uniquely identify each image.

        Raises:
            SystemExit: If duplicate filenames are found in the column.
        """
        # Count only non-missing values because empty filenames should not be treated as duplicates.
        if data_df.loc[data_df[filename_col].notna()].shape[0] != data_df[filename_col].nunique():
            sys.exit(f"{filename_col} is not a unique identifier for this dataset, please choose a column with unique values for filenames.")

    def handle_missing_filenames(self, data_df, filename_col, url_col):
        """
        Handle cases where URLs are present but filenames are missing.

        Parameters:
            data_df (pd.DataFrame): DataFrame loaded from the CSV file.
            filename_col (str): Name of the column containing image filenames.
            url_col (str): Name of the column containing image URLs.

        Returns:
            missing (pd.DataFrame) | None: DataFrame of entries with URLs but missing filenames or None.
        """
        # Find rows that have a URL but no filename, since those records cannot be downloaded safely.
        missing = data_df.loc[data_df[filename_col].isna() & data_df[url_col].notna()]
        if missing.empty:
            # No missing names were found, so there is nothing to report.
            return None
        if len(missing) <= 5:
            # Show the full set of problematic rows directly in the console for small cases.
            print("\n Missing filenames detected (showing all):")
            print(missing)
        else:
            # For larger problems, save the affected rows to disk so they can be fixed offline.
            csv_base = os.path.splitext(self.csv_path)[0]
            save_path = f"{csv_base}_missing_filenames.csv"
            missing.to_csv(save_path, index=False)
            print(
                f"\n Missing filenames detected for {len(missing)} rows.\n"
                f"Because there are more than 5, they were saved to:\n  {save_path}\n"
                f"Please correct the CSV and re-run."
            )
        # Return the flagged rows so the calling code can inspect or report them further.
        return missing
    
    def setup_expected_columns(self, args):
        """
        Build the expected column mapping used by the download workflow.

        Parameters:
            args (argparse.Namespace): Parsed command-line arguments.

        Returns:
            (expected_cols, subfolders) tuple[dict, str | None]: Column mapping to use for filename, URL, and optional subfolder handling.
        """
        # Normalize the CLI input into the internal column names used throughout the workflow.
        subfolders = args.subdir_col
        expected_cols = {
            "filename_col": args.img_name_col.lower(),
            "url_col": args.url_col.lower()
        }
        if subfolders:
            # Keep the subfolder column name consistent with the rest of the input handling.
            subfolders = subfolders.lower()
            expected_cols["subfolders"] = subfolders
        return expected_cols, subfolders

    def check_existing_images(self, csv_path, img_dir, source_df, filename_col, subfolders=None):
        """
        Checks which files from the CSV already exist in the image directory.

        Adds a new boolean column `in_img_dir` to source_df indicating which images
        are already in the directory.

        If all images already exist in the directory, the function will exit early
        by calling `sys.exit()`, and no further processing will occur.

        Parameters:
            csv_path (str): Path to the CSV file containing image information.
            img_dir (str): Path to the directory where images are to be stored.
            source_df (pd.DataFrame): DataFrame loaded from the CSV, containing image metadata.
            filename_col (str): Name of the column in source_df that contains image filenames.
            subfolders (str): Name of the column in source_df that contains subfolder names. (optional)

        Returns:
            updated_df (pd.DataFrame): DataFrame with new column 'in_img_dir' indicating presence in img_dir.
            filtered_df (pd.DataFrame): DataFrame filtered to only files not present in img_dir.
        """
        # Create a copy to avoid modifying the original DataFrame
        df = source_df.copy()

        if not os.path.exists(img_dir):
            # Directory doesn't exist, so nothing to check
            df["in_img_dir"] = False
            # Return the updated df and the filtered dataframe of items that still need downloading
            filtered_df = df[~df["in_img_dir"]].copy()
            return df, filtered_df

        try:
            existing_files = gather_file_paths(img_dir)
        except EmptyInputDirectoryError:
            # If the directory exists but is empty, sumbuddy raises an error.
            # We catch it and treat it as an empty file list.
            existing_files = []

        existing_full_paths = {os.path.normpath(os.path.relpath(f, img_dir)) for f in existing_files}

        if subfolders:
            # We use a generic join here, but the apply(os.path.normpath) below fixes it for the specific OS
            raw_paths = df[subfolders].astype(str) + os.sep + df[filename_col].astype(str)
            # This converts '/' to '\' on Windows, or vice versa, ensuring a match
            df["expected_path"] = raw_paths.apply(os.path.normpath)
        else:
            # Normalize even simple filenames just in case they contain pathing characters
            df["expected_path"] = df[filename_col].astype(str).apply(os.path.normpath)
        
        # Determine which expected paths physically exist
        expected_present = df["expected_path"].isin(existing_full_paths)
        df["in_img_dir"] = expected_present.copy()
        
        # Clean up the temporary column before returning.
        df = df.drop(columns=["expected_path"])
        
        # Create filtered DataFrame
        filtered_df = df[~df["in_img_dir"]].copy()

        # Exit if all images are already there
        if filtered_df.empty:
            sys.exit(f"'{img_dir}' already contains all images. Exited without executing.")
        else:
            # Print directory status message - pre-download
            num_existing = len(existing_files)
            print(f"There are {num_existing} of the desired files already in {img_dir}. Based on {csv_path}, {filtered_df.shape[0]} images should be downloaded.")

        return df, filtered_df

    def _preview_or_save(self, label, df):
        """
        Print a small preview of a DataFrame or save it to disk when the result is larger.

        Parameters:
            label (str): Descriptive prefix for the output file and console message.
            df (pd.DataFrame): DataFrame containing the rows to preview or save.
        """
        # Result sets are written to a CSV so the user can review them without clutter.
        csv_base = os.path.splitext(self.csv_path)[0]
        save_path = f"{csv_base}_{label}.csv"
        df.to_csv(save_path, index=False)
        print(
            f"\n {len(df)} {label.replace('_', ' ')} detected.\n"
            f"Full list saved to:\n  {save_path}\n"
        )
        
    def check_duplicate_checksums(self, data_df, hash_col, ignore_duplicates=False):
        """
        Detect duplicate checksum values and optionally block execution.

        Parameters:
            data_df (pd.DataFrame): DataFrame containing the checksum column.
            hash_col (str): Name of the checksum column to inspect.
            ignore_duplicates (bool): If True, allow execution to continue after reporting duplicates.

        Raises:
            SystemExit: If duplicate checksums are found and duplicates are not ignored.
        """
        # Only consider non-null hashes when searching for repeated values.
        dupes = (
            data_df[data_df[hash_col].notna()]
            .groupby(hash_col)
            .filter(lambda x: len(x) > 1)
        )

        if dupes.empty:
            # No repeated checksums were found, so the download process can continue.
            print(" No duplicate checksums detected.")
            return

        # If duplicates exist, preview or save
        self._preview_or_save("duplicate_checksums", dupes)

        if ignore_duplicates:
            # The user explicitly allowed duplicates, so the workflow reports them and proceeds.
            print(
                f"\n Duplicate checksums detected ({len(dupes)} rows), "
                f"but --ignore-duplicates was passed. Continuing.\n"
            )
            return

        # Default behavior: block execution
        sys.exit(
            " Duplicate checksums detected. "
            "Use --ignore-duplicates to allow downloading duplicates."
        )

    def print_download_summary(self, img_dir, downsample_dir, subfolders, num_images):
        """
        Print a summary of where images and downsized images will be saved.

        Parameters:
            img_dir (str): Destination directory for downloaded images.
            downsample_dir (str | None): Optional directory for resized images.
            subfolders (str | None): Optional subfolder naming column.
            num_images (int): Number of images that will be downloaded.
        """
        # Print the planned download layout so the user can confirm the output locations.
        print("\n Download Summary")
        print("--------------------")
        print(f"Images will be downloaded to: {img_dir}")

        if downsample_dir:
            print(f"Downsampled images will be saved to: {downsample_dir}")
        else:
            print("Downsampled images: not requested")

        if subfolders:
            print(f"Subfolders enabled: {subfolders}")
        else:
            print("Subfolders: none")

        print(f"Images to download: {num_images}\n")