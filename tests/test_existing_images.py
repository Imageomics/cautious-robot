import unittest
import pandas as pd
from unittest.mock import patch
from cautiousrobot.utils import check_existing_images


class TestCheckExistingImages(unittest.TestCase):
    def setUp(self):
        self.csv_path = "test_data.csv"
        self.img_dir = "test_images"
        self.filename_col = "filename"
        self.sample_df = pd.DataFrame({
            self.filename_col: ["a.jpg", "b.jpg", "c.jpg"]
        })

    @patch("cautiousrobot.utils.os.path.exists", return_value=False)
    def test_directory_does_not_exist(self, mock_exists):
        """If image directory doesn't exist, all images marked as not in directory."""
        updated_df, filtered_df = check_existing_images(
            self.csv_path, self.img_dir, self.sample_df, self.filename_col
        )

        self.assertFalse(any(updated_df["in_img_dir"]))
        self.assertEqual(len(filtered_df), len(self.sample_df))
        mock_exists.assert_called_once_with(self.img_dir)

    @patch("cautiousrobot.utils.os.path.exists", return_value=True)
    @patch("cautiousrobot.utils.gather_file_paths", return_value=["test_images/a.jpg"])
    @patch("cautiousrobot.utils.print")
    def test_some_files_exist(self, mock_print, mock_gather, mock_exists):
        """Should mark existing files correctly and print status."""
        updated_df, filtered_df = check_existing_images(
            self.csv_path, self.img_dir, self.sample_df, self.filename_col
        )

        self.assertTrue(updated_df.loc[0, "in_img_dir"])   # a.jpg exists
        self.assertFalse(updated_df.loc[1, "in_img_dir"])  # b.jpg missing
        self.assertFalse(updated_df.loc[2, "in_img_dir"])  # c.jpg missing
        self.assertEqual(len(filtered_df), 2)
        mock_print.assert_called_once()
        self.assertIn("There are 1 of the desired files", mock_print.call_args[0][0])

    @patch("cautiousrobot.utils.os.path.exists", return_value=True)
    @patch("cautiousrobot.utils.gather_file_paths", return_value=["test_images/a.jpg", "test_images/b.jpg", "test_images/c.jpg"])
    def test_all_files_exist_exits(self, mock_gather, mock_exists):
        """If all images exist, should exit early with proper message."""
        with self.assertRaises(SystemExit) as cm:
            check_existing_images(
                self.csv_path, self.img_dir, self.sample_df, self.filename_col
            )

        self.assertIn("already contains all images", cm.exception.code)
        mock_exists.assert_called_once_with(self.img_dir)

    @patch("cautiousrobot.utils.os.path.exists", return_value=True)
    @patch("cautiousrobot.utils.gather_file_paths", return_value=[])
    @patch("cautiousrobot.utils.print")
    def test_no_files_exist(self, mock_print, mock_gather, mock_exists):
        """If no files exist, should mark all as not in directory and print message."""
        updated_df, filtered_df = check_existing_images(
            self.csv_path, self.img_dir, self.sample_df, self.filename_col
        )

        self.assertFalse(any(updated_df["in_img_dir"]))
        self.assertEqual(len(filtered_df), len(self.sample_df))
        mock_print.assert_called_once()
        self.assertIn("There are 0 of the desired files", mock_print.call_args[0][0])

    @patch("cautiousrobot.utils.os.path.exists", return_value=True)
    @patch("cautiousrobot.utils.gather_file_paths", return_value=["test_images/species1/a.jpg", "test_images/shouldnotcount/b.jpg"])
    def test_subfolders_handling(self, mock_gather, mock_exists):
        """When `subfolders` is provided, expected paths should be constructed and matched."""
        sub_df = pd.DataFrame({
            "subfolder": ["species1", "species2"],
            self.filename_col: ["a.jpg", "b.jpg"]
        })

        updated_df, filtered_df = check_existing_images(
            self.csv_path, self.img_dir, sub_df, self.filename_col, subfolders="subfolder"
        )

        # species1/a.jpg should be marked present, species2/b.jpg missing
        self.assertTrue(updated_df.loc[0, "in_img_dir"])
        self.assertFalse(updated_df.loc[1, "in_img_dir"])
        self.assertEqual(len(filtered_df), 1)


if __name__ == "__main__":
    unittest.main()
