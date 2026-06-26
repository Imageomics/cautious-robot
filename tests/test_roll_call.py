import unittest
from unittest.mock import patch
import pandas as pd

from cautiousrobot.roll_call import RollCall
from sumbuddy.exceptions import EmptyInputDirectoryError


class TestRollCall(unittest.TestCase):

    # ---------------------------------------------------------
    # validate_csv_extension
    # ---------------------------------------------------------
    def test_validate_csv_extension_valid(self):
        rc = RollCall()
        rc.validate_csv_extension("file.csv")  # should not raise

    def test_validate_csv_extension_uppercase(self):
        rc = RollCall()
        rc.validate_csv_extension("FILE.CSV")  # should not raise

    def test_validate_csv_extension_invalid(self):
        rc = RollCall()
        with self.assertRaises(SystemExit):
            rc.validate_csv_extension("file.txt")

    # ---------------------------------------------------------
    # validate_filename_uniqueness
    # ---------------------------------------------------------
    def test_validate_filename_uniqueness_unique(self):
        df = pd.DataFrame({"file": ["a.jpg", "b.jpg", "c.jpg"]})
        rc = RollCall()
        rc.validate_filename_uniqueness(df, "file")

    def test_validate_filename_uniqueness_all_null(self):
        df = pd.DataFrame({"file": [None, None]})
        rc = RollCall()
        rc.validate_filename_uniqueness(df, "file")  # should not raise

    def test_validate_filename_uniqueness_duplicates(self):
        df = pd.DataFrame({"file": ["a.jpg", "a.jpg", "b.jpg"]})
        rc = RollCall()
        with self.assertRaises(SystemExit):
            rc.validate_filename_uniqueness(df, "file")

    # ---------------------------------------------------------
    # handle_missing_filenames
    # ---------------------------------------------------------
    def test_handle_missing_filenames_none_multiple(self):
        df = pd.DataFrame({
            "file": ["a.jpg", "b.jpg"],
            "url": ["x", "y"]
        })
        rc = RollCall(csv_path="dummy.csv")
        result = rc.handle_missing_filenames(df, "file", "url")
        self.assertIsNone(result)

    def test_handle_missing_filenames_some_missing(self):
        df = pd.DataFrame({
            "file": ["a.jpg", None, "c.jpg"],
            "url": ["x", "y", "z"]
        })
        rc = RollCall(csv_path="dummy.csv")
        result = rc.handle_missing_filenames(df, "file", "url")
        self.assertEqual(len(result), 1)

    def test_handle_missing_filenames_all_missing(self):
        df = pd.DataFrame({
            "file": [None, None],
            "url": ["x", "y"]
        })
        rc = RollCall(csv_path="dummy.csv")
        result = rc.handle_missing_filenames(df, "file", "url")
        self.assertEqual(len(result), 2)

    # ---------------------------------------------------------
    # setup_expected_columns
    # ---------------------------------------------------------
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

    # ---------------------------------------------------------
    # check_existing_images
    # ---------------------------------------------------------
    @patch("os.path.exists", return_value=False)
    def test_check_existing_images_directory_missing(self, mock_exists):
        df = pd.DataFrame({"file": ["a.jpg", "b.jpg"]})
        rc = RollCall(csv_path="dummy.csv")
        full_df, filtered = rc.check_existing_images(
            "dummy.csv", "missing_dir", df, "file"
        )
        self.assertEqual(len(filtered), 2)

    @patch("cautiousrobot.roll_call.gather_file_paths")
    def test_check_existing_images_empty_directory(self, mock_gather):
        mock_gather.side_effect = EmptyInputDirectoryError("fake_dir")

        df = pd.DataFrame({"file": ["a.jpg", "b.jpg"]})
        rc = RollCall(csv_path="dummy.csv")

        full_df, filtered = rc.check_existing_images(
            "dummy.csv", ".", df, "file"
        )

        self.assertEqual(len(filtered), 2)

    @patch("cautiousrobot.roll_call.gather_file_paths")
    def test_check_existing_images_none_exist(self, mock_gather):
        mock_gather.return_value = []
        df = pd.DataFrame({"file": ["a.jpg", "b.jpg"]})
        rc = RollCall(csv_path="dummy.csv")
        full_df, filtered = rc.check_existing_images(
            "dummy.csv", ".", df, "file"
        )
        self.assertEqual(len(filtered), 2)

    @patch("cautiousrobot.roll_call.gather_file_paths")
    def test_check_existing_images_some_exist(self, mock_gather):
        mock_gather.return_value = ["a.jpg"]
        df = pd.DataFrame({"file": ["a.jpg", "b.jpg"]})
        rc = RollCall(csv_path="dummy.csv")
        full_df, filtered = rc.check_existing_images(
            "dummy.csv", ".", df, "file"
        )
        self.assertEqual(len(filtered), 1)
        self.assertIn("b.jpg", filtered["file"].values)

    @patch("cautiousrobot.roll_call.gather_file_paths")
    def test_check_existing_images_subfolders_all_exist(self, mock_gather):
        mock_gather.return_value = ["cat/a.jpg"]

        df = pd.DataFrame({"file": ["a.jpg"], "category": ["cat"]})
        rc = RollCall(csv_path="dummy.csv")

        with self.assertRaises(SystemExit):
            rc.check_existing_images(
                "dummy.csv", ".", df, "file", subfolders="category"
            )


    @patch("cautiousrobot.roll_call.gather_file_paths")
    def test_check_existing_images_subfolders(self, mock_gather):
        mock_gather.return_value = []  # nothing exists

        df = pd.DataFrame({"file": ["a.jpg"], "category": ["cat"]})
        rc = RollCall(csv_path="dummy.csv")

        full_df, filtered = rc.check_existing_images(
            "dummy.csv", ".", df, "file", subfolders="category"
        )

        self.assertEqual(len(filtered), 1)
        self.assertIn("a.jpg", filtered["file"].values)


    # ---------------------------------------------------------
    # check_duplicate_checksums
    # ---------------------------------------------------------
    def test_check_duplicate_checksums_none(self):
        df = pd.DataFrame({"hash": ["a", "b", "c"]})
        rc = RollCall()
        rc.check_duplicate_checksums(df, "hash")

    def test_check_duplicate_checksums_some(self):
        df = pd.DataFrame({"hash": ["a", "a", "b"]})
        rc = RollCall(csv_path="dummy.csv")
        with self.assertRaises(SystemExit):
            rc.check_duplicate_checksums(df, "hash")

    def test_check_duplicate_checksums_ignore(self):
        df = pd.DataFrame({"hash": ["a", "a", "b"]})
        rc = RollCall(csv_path="dummy.csv")
        rc.check_duplicate_checksums(df, "hash", ignore_duplicates=True)

    def test_check_duplicate_checksums_multiple_groups(self):
        df = pd.DataFrame({"hash": ["a", "a", "b", "b"]})
        rc = RollCall(csv_path="dummy.csv")
        with self.assertRaises(SystemExit):
            rc.check_duplicate_checksums(df, "hash")

    # ---------------------------------------------------------
    # _preview_or_save
    # ---------------------------------------------------------
    @patch("builtins.print")
    def test_preview_or_save_small(self, mock_print):
        df = pd.DataFrame({"x": [1, 2]})
        rc = RollCall(csv_path="dummy.csv")
        rc._preview_or_save("test_label", df)
        self.assertTrue(mock_print.called)

    @patch("pandas.DataFrame.to_csv")
    @patch("builtins.print")
    def test_preview_or_save_large(self, mock_print, mock_csv):
        df = pd.DataFrame({"x": list(range(10))})
        rc = RollCall(csv_path="dummy.csv")
        rc._preview_or_save("test_label", df)
        mock_csv.assert_called_once()


if __name__ == "__main__":
    unittest.main()
