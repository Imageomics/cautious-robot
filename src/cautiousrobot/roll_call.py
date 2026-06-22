# CSV validation and input handling functions.

import os
import sys
import logging
from sumbuddy import gather_file_paths
from sumbuddy.exceptions import EmptyInputDirectoryError

logger = logging.getLogger(__name__)


class RollCall:
    """Pre-download validation and directory verification."""

    def __init__(self, csv_path=None):
        self.csv_path = csv_path

    def validate_csv_extension(self, csv_path):
        """Validate that the input file has a .csv extension."""
        if not csv_path.lower().endswith(".csv"):
            msg = "Expected CSV for input file; extension should be '.csv'"
            logger.error(msg)
            sys.exit(msg)

    def validate_filename_uniqueness(self, data_df, filename_col):
        """Validate that the filename column contains unique values."""
        if data_df.loc[data_df[filename_col].notna()].shape[0] != data_df[filename_col].nunique():
            msg = (
                f"{filename_col} is not a unique identifier for this dataset, "
                "please choose a column with unique values for filenames."
            )
            logger.error(msg)
            sys.exit(msg)

    def handle_missing_filenames(self, data_df, filename_col, url_col):
        """Handle cases where URLs exist but filenames are missing."""
        missing = data_df.loc[data_df[filename_col].isna() & data_df[url_col].notna()]

        if missing.empty:
            logger.info("No missing filenames detected.")
            return None

        if len(missing) <= 5:
            logger.warning(f"Missing filenames detected ({len(missing)} rows).")
            print("\n Missing filenames detected (showing all):")
            print(missing)
        else:
            csv_base = os.path.splitext(self.csv_path)[0]
            save_path = f"{csv_base}_missing_filenames.csv"
            missing.to_csv(save_path, index=False)

            logger.warning(
                f"{len(missing)} missing filenames detected. Saved to {save_path}"
            )

            print(
                f"\n Missing filenames detected for {len(missing)} rows.\n"
                f"Because there are more than 5, they were saved to:\n  {save_path}\n"
                f"Please correct the CSV and re-run."
            )

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

        logger.info("Expected columns configured.")
        return expected_cols, subfolders

    def check_existing_images(self, csv_path, img_dir, source_df, filename_col, subfolders=None):
        """Checks which files from the CSV already exist in the image directory."""
        df = source_df.copy()

        if not os.path.exists(img_dir):
            logger.info(f"Image directory '{img_dir}' does not exist; all images will be downloaded.")
            df["in_img_dir"] = False
            return df, df

        try:
            existing_files = gather_file_paths(img_dir)
        except EmptyInputDirectoryError:
            logger.warning(f"Image directory '{img_dir}' is empty.")
            existing_files = []

        existing_full_paths = {
            os.path.normpath(os.path.relpath(f, img_dir))
            for f in existing_files
        }

        if subfolders:
            raw_paths = df[subfolders].astype(str) + os.sep + df[filename_col].astype(str)
            df["expected_path"] = raw_paths.apply(os.path.normpath)
        else:
            df["expected_path"] = df[filename_col].astype(str).apply(os.path.normpath)

        df["in_img_dir"] = df["expected_path"].isin(existing_full_paths)
        df = df.drop(columns=["expected_path"])
        filtered_df = df[~df["in_img_dir"]].copy()

        if filtered_df.empty:
            msg = f"'{img_dir}' already contains all images. Exited without executing."
            logger.info(msg)
            sys.exit(msg)

        num_existing = len(existing_files)
        logger.info(
            f"{num_existing} files already exist in {img_dir}. "
            f"{filtered_df.shape[0]} images still need downloading."
        )

        print(
            f"There are {num_existing} of the desired files already in {img_dir}. "
            f"Based on {csv_path}, {filtered_df.shape[0]} images should be downloaded."
        )

        return df, filtered_df

    def _preview_or_save(self, label, df):
        """Print up to 5 rows or save to CSV if larger."""
        if len(df) <= 5:
            logger.warning(f"{label} detected ({len(df)} rows). Previewing.")
            print(f"\n❗ {label.replace('_', ' ').title()} detected (showing all):")
            print(df)
        else:
            csv_base = os.path.splitext(self.csv_path)[0]
            save_path = f"{csv_base}_{label}.csv"
            df.to_csv(save_path, index=False)

            logger.warning(f"{len(df)} {label} detected. Saved to {save_path}")

            print(
                f"\n❗ {len(df)} {label.replace('_', ' ')} detected.\n"
                f"Full list saved to:\n  {save_path}\n"
            )

    def check_duplicate_checksums(self, data_df, hash_col, ignore_duplicates=False):
        """Detect duplicate checksum values and optionally block execution."""
        dupes = (
            data_df[data_df[hash_col].notna()]
            .groupby(hash_col)
            .filter(lambda x: len(x) > 1)
        )

        if dupes.empty:
            logger.info("No duplicate checksums detected.")
            print("No duplicate checksums detected.")
            return

        logger.warning(f"Duplicate checksums detected ({len(dupes)} rows).")
        self._preview_or_save("duplicate_checksums", dupes)

        if ignore_duplicates:
            logger.warning("Ignoring duplicates due to --ignore-duplicates flag.")
            print(
                f"\n Duplicate checksums detected ({len(dupes)} rows), "
                f"but --ignore-duplicates was passed. Continuing.\n"
            )
            return

        msg = (
            " Duplicate checksums detected. "
            "Use --ignore-duplicates to allow downloading duplicates."
        )
        logger.error(msg)
        sys.exit(msg)

    def print_download_summary(self, img_dir, downsample_dir, subfolders, num_images):
        """Print a summary of where images and downsized images will be saved."""
        logger.info("Printing download summary.")

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
