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

    # ---------------------------------------------------------
    # CSV extension validation
    # ---------------------------------------------------------
    def validate_csv_extension(self, csv_path):
        """Validate that the input file has a .csv extension."""
        if not csv_path.lower().endswith(".csv"):
            msg = "Expected CSV for input file; extension should be '.csv'"
            logger.error(msg)
            sys.exit(msg)

    # ---------------------------------------------------------
    # Filename uniqueness validation
    # ---------------------------------------------------------
    def validate_filename_uniqueness(self, data_df, filename_col):
        """Validate that the filename column contains unique values."""
        non_null = data_df[filename_col].dropna()
        if non_null.nunique() != len(non_null):
            msg = (
                f"{filename_col} is not a unique identifier for this dataset, "
                "please choose a column with unique values for filenames."
            )
            logger.error(msg)
            sys.exit(msg)

    # ---------------------------------------------------------
    # Missing filename handling
    # ---------------------------------------------------------
    def handle_missing_filenames(self, data_df, filename_col, url_col):
        """Handle cases where URLs exist but filenames are missing."""
        missing = data_df.loc[data_df[filename_col].isna() & data_df[url_col].notna()]

        if missing.empty:
            logger.info("No missing filenames detected.")
            return None

        count = len(missing)

        if count <= 5:
            logger.info(f"Missing filenames detected in {count} rows — previewing.")
            print("\nMissing filenames detected (showing all):")
            print(missing)
        else:
            csv_base = os.path.splitext(self.csv_path)[0]
            save_path = f"{csv_base}_missing_filenames.csv"
            missing.to_csv(save_path, index=False)

            logger.info(
                f"Missing filenames detected in {count} rows — saved to {save_path}."
            )

            print(
                f"\nMissing filenames detected for {count} rows.\n"
                f"Because there are more than 5, they were saved to:\n  {save_path}\n"
                f"Please correct the CSV and re-run."
            )

        return missing

    # ---------------------------------------------------------
    # Expected column setup
    # ---------------------------------------------------------
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

    # ---------------------------------------------------------
    # Existing image detection
    # ---------------------------------------------------------
    def check_existing_images(self, csv_path, img_dir, source_df, filename_col, subfolders=None):
        """Checks which files from the CSV already exist in the image directory."""
        df = source_df.copy()

        # Case 1: Directory does not exist
        if not os.path.exists(img_dir):
            logger.info(
                f"Image directory '{img_dir}' does not exist; all images will be downloaded."
            )
            df["in_img_dir"] = False
            return df, df.copy()

        # Case 2: Directory exists but may be empty
        try:
            existing_files = gather_file_paths(img_dir)
        except EmptyInputDirectoryError:
            logger.info(
                f"Image directory '{img_dir}' is empty; all images will be downloaded."
            )
            existing_files = []

        # Normalize existing paths relative to img_dir
        existing_full_paths = {
            os.path.normpath(os.path.relpath(f, img_dir))
            for f in existing_files
        }

        # Build expected relative paths
        if subfolders:
            raw_paths = (
                df[subfolders].astype(str) + os.sep + df[filename_col].astype(str)
            )
            df["expected_path"] = raw_paths.apply(os.path.normpath)
        else:
            df["expected_path"] = df[filename_col].astype(str).apply(os.path.normpath)

        # Compare expected vs actual
        df["in_img_dir"] = df["expected_path"].isin(existing_full_paths)
        df = df.drop(columns=["expected_path"])
        filtered_df = df[~df["in_img_dir"]].copy()

        # Case 3: All images already present
        if filtered_df.empty:
            msg = (
                f"'{img_dir}' already contains all images listed in '{csv_path}'. Exiting."
            )
            logger.info(msg)
            sys.exit(msg)

        # Summary logging
        num_present = df["in_img_dir"].sum()
        num_missing = filtered_df.shape[0]

        logger.info(
            f"{num_present} of the images listed in '{csv_path}' already exist in '{img_dir}'. "
            f"{num_missing} images still need downloading."
        )

        print(
            f"There are {num_present} of the desired files already in {img_dir}. "
            f"Based on {csv_path}, {num_missing} images should be downloaded."
        )

        return df, filtered_df

    # ---------------------------------------------------------
    # Preview or save helper
    # ---------------------------------------------------------
    def _preview_or_save(self, label, df):
        """Print up to 5 rows or save to CSV if larger."""
        count = len(df)

        if count <= 5:
            logger.info(
                f"{label.replace('_', ' ')} detected in {count} rows — previewing."
            )
            print(f"\n{label.replace('_', ' ').title()} detected (showing all):")
            print(df)
            return

        # Save for larger sets
        csv_base = os.path.splitext(self.csv_path)[0]
        save_path = f"{csv_base}_{label}.csv"
        df.to_csv(save_path, index=False)

        logger.info(
            f"{label.replace('_', ' ')} detected in {count} rows — saved to {save_path}."
        )

        print(
            f"\n{count} {label.replace('_', ' ')} detected.\n"
            f"Full list saved to:\n  {save_path}\n"
        )

    # ---------------------------------------------------------
    # Duplicate checksum detection
    # ---------------------------------------------------------
    def check_duplicate_checksums(self, data_df, hash_col, ignore_duplicates=False):
        """Detect duplicate checksum values and optionally block execution."""

        dupes = (
            data_df[data_df[hash_col].notna()]
            .groupby(hash_col)
            .filter(lambda x: len(x) > 1)
        )

        if dupes.empty:
            logger.info("No duplicate checksums detected.")
            print("✔ No duplicate checksums detected.")
            return

        count = len(dupes)
        logger.warning(f"Duplicate checksums detected in {count} rows.")
        self._preview_or_save("duplicate_checksums", dupes)

        if ignore_duplicates:
            logger.warning("Ignoring duplicate checksums due to --ignore-duplicates flag.")
            print(
                f"\n⚠ Duplicate checksums detected in {count} rows, "
                f"but --ignore-duplicates was passed. Continuing.\n"
            )
            return

        msg = (
            f"❗ Duplicate checksums detected in {count} rows. "
            "Use --ignore-duplicates to allow downloading duplicates."
        )
        logger.error(msg)
        sys.exit(msg)

    # ---------------------------------------------------------
    # Download summary
    # ---------------------------------------------------------
    def print_download_summary(self, img_dir, downsample_dir, subfolders, num_images):
        """Print a summary of where images and downsized images will be saved."""
        logger.info("Printing download summary.")

        print("\nDownload Summary")
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
