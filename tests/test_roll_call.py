import unittest
from unittest.mock import patch
import pandas as pd

from cautiousrobot.roll_call import RollCall


class TestRollCall(unittest.TestCase):

    # -----------------------------
    # validate_csv_extension
    # -----------------------------
    def test_validate_csv_extension_valid(self):
        rc = RollCall()
        rc.validate_csv_extension("data.csv")  # should not raise

    def test_validate_csv_extension_invalid(self):
        rc = RollCall()
        with self.assertRaises(SystemExit):
            rc.validate_csv_extension("data.txt")

    # -----------------------------
    # validate_filename_uniqueness
    # -----------------------------
    def test_validate_filename_uniqueness_unique(self):
        df = pd.DataFrame({"file": ["a.jpg", "b.jpg", "c.jpg"]})
        rc = RollCall()
        rc.validate_filename_uniqueness(df, "file")  # should not raise

    def test_validate_filename_uniqueness_not_unique(self):
        df = pd.DataFrame({"file": ["a.jpg", "a.jpg", "b.jpg"]})
        rc = RollCall()
        with self.assertRaises(SystemExit):
            rc.validate_filename_uniqueness(df, "file")

    # -----------------------------
    # handle_missing_filenames
    # -----------------------------
    def test_handle_missing_filenames_none_missing(self):
        df = pd.DataFrame({"file": ["a.jpg"], "url": ["http://x"]})
        rc = RollCall(csv_path="dummy.csv")
        result = rc.handle_missing_filenames(df, "file", "url")
        self.assertIsNone(result)

    def test_handle_missing_filenames_some_missing(self):
        df = pd.DataFrame({
            "file": [None, None],
            "url": ["http://x", "http://y"]
        })
        rc = RollCall(csv_path="dummy.csv")
        result = rc.handle_missing_filenames(df, "file", "url")
        self.assertEqual(len(result), 2)

    # -----------------------------
    # setup_expected_columns
    # -----------------------------
    class Args:
        img_name_col = "FILENAME"
        url_col = "URL"
        subdir_col = "CATEGORY"

    def test_setup_expected_columns(self):
        rc = RollCall()
        expected, subfolders = rc.setup_expected_columns(self.Args)

        self.assertEqual(expected["filename_col"], "filename")
        self.assertEqual(expected["url_col"], "url")
        self.assertEqual(expected["subfolders"], "category")
        self.assertEqual(subfolders, "category")

    # -----------------------------
    # check_existing_images
    # -----------------------------
    @patch("cautiousrobot.roll_call.gather_file_paths")
    def test_check_existing_images_all_missing(self, mock_gather):
        mock_gather.return_value = []
        df = pd.DataFrame({"file": ["a.jpg", "b.jpg"]})

        rc = RollCall(csv_path="dummy.csv")
        full_df, filtered = rc.check_existing_images(
            csv_path="dummy.csv",
            img_dir="fake_dir",
            source_df=df,
            filename_col="file"
        )

        self.assertEqual(len(filtered), 2)

    @patch("cautiousrobot.roll_call.gather_file_paths")
    def test_check_existing_images_some_exist(self, mock_gather):
        mock_gather.return_value = ["a.jpg"]

        df = pd.DataFrame({"file": ["a.jpg", "b.jpg"]})

        rc = RollCall(csv_path="dummy.csv")
        full_df, filtered = rc.check_existing_images(
            csv_path="dummy.csv",
            img_dir=".",
            source_df=df,
            filename_col="file"
        )

        self.assertEqual(len(filtered), 1)
        self.assertIn("b.jpg", filtered["file"].values)

    # -----------------------------
    # check_duplicate_checksums
    # -----------------------------
    def test_check_duplicate_checksums_none(self):
        df = pd.DataFrame({"hash": ["aaa", "bbb", "ccc"]})
        rc = RollCall()
        rc.check_duplicate_checksums(df, "hash")  # should not raise

    def test_check_duplicate_checksums_detected_exit(self):
        df = pd.DataFrame({"hash": ["aaa", "aaa", "bbb"]})
        rc = RollCall(csv_path="dummy.csv")
        with self.assertRaises(SystemExit):
            rc.check_duplicate_checksums(df, "hash", ignore_duplicates=False)

    def test_check_duplicate_checksums_ignore(self):
        df = pd.DataFrame({"hash": ["aaa", "aaa", "bbb"]})
        rc = RollCall(csv_path="dummy.csv")
        rc.check_duplicate_checksums(df, "hash", ignore_duplicates=True)  # should not raise


if __name__ == "__main__":
    unittest.main()
