# CSV validation and input handling functions.

import os
import sys
from sumbuddy import gather_file_paths
from sumbuddy.exceptions import EmptyInputDirectoryError


class RollCall:
    """Pre-download validation and directory verification."""

    def __init__(self, csv_path=None):
        self.csv_path = csv_path

    def validate_csv_extension(self, csv_path):
        """Validate that the input file has a .csv extension."""
        if not csv_path.lower().endswith(".csv"):
            sys.exit("Expected CSV for input file; extension should be '.csv'")

    def validate_filename_uniqueness(self, data_df, filename_col):
        """Validate that the filename column contains unique values."""
        if data_df.loc[data_df[filename_col].notna()].shape[0] != data_df[filename_col].nunique():
            sys.exit(f"{filename_col} is not a unique identifier for this dataset, please choose a column with unique values for filenames.")

    def handle_missing_filenames(self, data_df, filename_col, url_col):
        """Handle cases where URLs exist but filenames are missing."""
        # Rows where filename is missing but URL exists
        missing = data_df.loc[data_df[filename_col].isna() & data_df[url_col].notna()]
        if missing.empty:
            return None
        if len(missing) <= 5:
            print("\n❗ Missing filenames detected (showing all):")
            print(missing)
        else:
            # Save path follows BuddyCheck pattern: <csv_basename>_missing_filenames.csv
            csv_base = os.path.splitext(self.csv_path)[0]
            save_path = f"{csv_base}_missing_filenames.csv"
            missing.to_csv(save_path, index=False)
            print(
                f"\n❗ Missing filenames detected for {len(missing)} rows.\n"
                f"Because there are more than 5, they were saved to:\n  {save_path}\n"
                f"Please correct the CSV and re-run."
            )
        # Return missing rows for reference
        return missing
    
    def setup_expected_columns(self, args):
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

    def check_existing_images(self, csv_path, img_dir, source_df, filename_col, subfolders=None):
        """Checks which files from the CSV already exist in the image directory."""
        df = source_df.copy()

        if not os.path.exists(img_dir):
            df["in_img_dir"] = False
            filtered_df = df[~df["in_img_dir"]].copy()
            return df, filtered_df

        try:
            existing_files = gather_file_paths(img_dir)
        except EmptyInputDirectoryError:
            existing_files = []

        existing_full_paths = {os.path.normpath(os.path.relpath(f, img_dir)) for f in existing_files}

        if subfolders:
            raw_paths = df[subfolders].astype(str) + os.sep + df[filename_col].astype(str)
            df["expected_path"] = raw_paths.apply(os.path.normpath)
        else:
            df["expected_path"] = df[filename_col].astype(str).apply(os.path.normpath)

        expected_present = df["expected_path"].isin(existing_full_paths)
        df["in_img_dir"] = expected_present.copy()
        df = df.drop(columns=["expected_path"])
        filtered_df = df[~df["in_img_dir"]].copy()

        if filtered_df.empty:
            sys.exit(f"'{img_dir}' already contains all images. Exited without executing.")
        else:
            num_existing = len(existing_files)
            print(f"There are {num_existing} of the desired files already in {img_dir}. Based on {csv_path}, {filtered_df.shape[0]} images should be downloaded.")

        return df, filtered_df

    def _preview_or_save(self, label, df):
        """Print up to 5 rows or save to CSV if larger."""
        if len(df) <= 5:
            print(f"\n❗ {label.replace('_', ' ').title()} detected (showing all):")
            print(df)
        else:
            csv_base = os.path.splitext(self.csv_path)[0]
            save_path = f"{csv_base}_{label}.csv"
            df.to_csv(save_path, index=False)
            print(
                f"\n❗ {len(df)} {label.replace('_', ' ')} detected.\n"
                f"Full list saved to:\n  {save_path}\n"
            )
            
    def check_duplicate_checksums(self, data_df, hash_col, ignore_duplicates=False):
        """Detect duplicate checksum values and optionally block execution."""

        # Only consider non-null hashes
        dupes = (
            data_df[data_df[hash_col].notna()]
            .groupby(hash_col)
            .filter(lambda x: len(x) > 1)
        )

        if dupes.empty:
            print("✔ No duplicate checksums detected.")
            return

        # If duplicates exist, preview or save
        self._preview_or_save("duplicate_checksums", dupes)

        if ignore_duplicates:
            print(
                f"\n⚠ Duplicate checksums detected ({len(dupes)} rows), "
                f"but --ignore-duplicates was passed. Continuing.\n"
            )
            return

        # Default behavior: block execution
        sys.exit(
            "❗ Duplicate checksums detected. "
            "Use --ignore-duplicates to allow downloading duplicates."
        )

    def print_download_summary(self, img_dir, downsample_dir, subfolders, num_images):
        """Print a summary of where images and downsized images will be saved."""
        print("\n📦 Download Summary")
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