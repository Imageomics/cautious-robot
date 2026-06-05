import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys

from cautiousrobot.validation import (
    validate_csv_extension,
    validate_filename_uniqueness,
    handle_missing_filenames,
    setup_expected_columns,
)


class TestValidateCsvExtension(unittest.TestCase):
    """Test CSV file extension validation."""

    def test_valid_csv_extension(self):
        """Should not raise exception for valid .csv file."""
        # Should not raise any exception
        try:
            validate_csv_extension("data.csv")
        except SystemExit:
            self.fail("validate_csv_extension raised SystemExit unexpectedly")

    def test_csv_extension_case_sensitive(self):
        """Should accept .csv regardless of path content."""
        try:
            validate_csv_extension("MyData.csv")
            validate_csv_extension("path/to/data.csv")
        except SystemExit:
            self.fail("validate_csv_extension raised SystemExit unexpectedly")

    def test_invalid_txt_extension(self):
        """Should exit if file has .txt extension."""
        with self.assertRaises(SystemExit) as cm:
            validate_csv_extension("data.txt")
        self.assertIn("csv", cm.exception.code.lower())

    def test_invalid_xlsx_extension(self):
        """Should exit if file has .xlsx extension."""
        with self.assertRaises(SystemExit) as cm:
            validate_csv_extension("data.xlsx")
        self.assertIn("csv", cm.exception.code.lower())

    def test_invalid_no_extension(self):
        """Should exit if file has no extension."""
        with self.assertRaises(SystemExit) as cm:
            validate_csv_extension("data")
        self.assertIn("csv", cm.exception.code.lower())

    def test_invalid_csv_as_substring(self):
        """Should require .csv as actual extension, not substring."""
        with self.assertRaises(SystemExit) as cm:
            validate_csv_extension("data.csv.backup")
        self.assertIn("csv", cm.exception.code.lower())


class TestValidateFilenameUniqueness(unittest.TestCase):
    """Test filename uniqueness validation."""

    def test_unique_filenames(self):
        """Should not raise exception for unique filenames."""
        df = pd.DataFrame({
            "filename": ["image1.jpg", "image2.jpg", "image3.jpg"]
        })
        try:
            validate_filename_uniqueness(df, "filename")
        except SystemExit:
            self.fail("validate_filename_uniqueness raised SystemExit unexpectedly")

    def test_duplicate_filenames(self):
        """Should exit if filenames are not unique."""
        df = pd.DataFrame({
            "filename": ["image1.jpg", "image1.jpg", "image2.jpg"]
        })
        with self.assertRaises(SystemExit) as cm:
            validate_filename_uniqueness(df, "filename")
        self.assertIn("unique identifier", cm.exception.code.lower())

    def test_filenames_with_missing_values(self):
        """Should only count non-null filenames for uniqueness."""
        df = pd.DataFrame({
            "filename": ["image1.jpg", None, "image2.jpg", None]
        })
        try:
            validate_filename_uniqueness(df, "filename")
        except SystemExit:
            self.fail("validate_filename_uniqueness raised SystemExit unexpectedly")

    def test_all_missing_filenames(self):
        """Should not raise exception if all filenames are missing."""
        df = pd.DataFrame({
            "filename": [None, None, None]
        })
        try:
            validate_filename_uniqueness(df, "filename")
        except SystemExit:
            self.fail("validate_filename_uniqueness raised SystemExit unexpectedly")

    def test_single_filename(self):
        """Should not raise exception for single unique filename."""
        df = pd.DataFrame({
            "filename": ["image1.jpg"]
        })
        try:
            validate_filename_uniqueness(df, "filename")
        except SystemExit:
            self.fail("validate_filename_uniqueness raised SystemExit unexpectedly")

    def test_empty_dataframe(self):
        """Should not raise exception for empty dataframe."""
        df = pd.DataFrame({
            "filename": []
        })
        try:
            validate_filename_uniqueness(df, "filename")
        except SystemExit:
            self.fail("validate_filename_uniqueness raised SystemExit unexpectedly")

    def test_custom_column_name(self):
        """Should work with custom column names."""
        df = pd.DataFrame({
            "image_name": ["photo1.jpg", "photo2.jpg", "photo3.jpg"]
        })
        try:
            validate_filename_uniqueness(df, "image_name")
        except SystemExit:
            self.fail("validate_filename_uniqueness raised SystemExit unexpectedly")

    def test_duplicate_with_custom_column(self):
        """Should detect duplicates in custom column names."""
        df = pd.DataFrame({
            "image_id": ["img_001", "img_001", "img_002"]
        })
        with self.assertRaises(SystemExit) as cm:
            validate_filename_uniqueness(df, "image_id")
        self.assertIn("unique identifier", cm.exception.code.lower())


class TestHandleMissingFilenames(unittest.TestCase):
    """Test handling of missing filename values."""

    def setUp(self):
        """Set up test fixtures."""
        self.filename_col = "filename"
        self.url_col = "file_url"

    def test_no_missing_filenames(self):
        """Should not prompt if all filenames are present."""
        df = pd.DataFrame({
            self.filename_col: ["image1.jpg", "image2.jpg"],
            self.url_col: ["http://url1.com", "http://url2.com"]
        })
        with patch("builtins.input") as mock_input:
            handle_missing_filenames(df, self.filename_col, self.url_col)
            mock_input.assert_not_called()

    def test_missing_filenames_with_urls_user_accepts(self):
        """Should continue when user answers 'y' to missing filenames prompt."""
        df = pd.DataFrame({
            self.filename_col: ["image1.jpg", None, "image3.jpg"],
            self.url_col: ["http://url1.com", "http://url2.com", "http://url3.com"]
        })
        with patch("builtins.input", return_value="y"):
            try:
                handle_missing_filenames(df, self.filename_col, self.url_col)
            except SystemExit:
                self.fail("Should not exit when user answers 'y'")

    def test_missing_filenames_with_urls_user_declines(self):
        """Should exit when user answers 'n' to missing filenames prompt."""
        df = pd.DataFrame({
            self.filename_col: ["image1.jpg", None, "image3.jpg"],
            self.url_col: ["http://url1.com", "http://url2.com", "http://url3.com"]
        })
        with patch("builtins.input", return_value="n"):
            with self.assertRaises(SystemExit) as cm:
                handle_missing_filenames(df, self.filename_col, self.url_col)
            self.assertIn("Exited", cm.exception.code)

    def test_missing_filenames_user_input_case_insensitive(self):
        """Should accept 'Y' or other case variations for yes response."""
        df = pd.DataFrame({
            self.filename_col: ["image1.jpg", None],
            self.url_col: ["http://url1.com", "http://url2.com"]
        })
        with patch("builtins.input", return_value="Y"):
            try:
                handle_missing_filenames(df, self.filename_col, self.url_col)
            except SystemExit:
                self.fail("Should accept 'Y' as valid yes response")

    def test_missing_filenames_prompt_shows_count(self):
        """Should show number of missing filenames in prompt."""
        df = pd.DataFrame({
            self.filename_col: ["img1.jpg", None, None, "img4.jpg"],
            self.url_col: ["url1", "url2", "url3", "url4"]
        })
        with patch("builtins.input", return_value="n") as mock_input:
            with self.assertRaises(SystemExit):
                handle_missing_filenames(df, self.filename_col, self.url_col)
            # Check that the input prompt includes the count
            call_args = mock_input.call_args[0][0]
            self.assertIn("2", call_args)

    def test_missing_filenames_no_urls(self):
        """Should not prompt if no URLs exist (no missing filenames to handle)."""
        df = pd.DataFrame({
            self.filename_col: ["image1.jpg", None, "image3.jpg"],
            self.url_col: [None, None, None]
        })
        with patch("builtins.input") as mock_input:
            handle_missing_filenames(df, self.filename_col, self.url_col)
            mock_input.assert_not_called()

    def test_missing_filenames_mixed_urls_and_filenames(self):
        """Should only count rows with URLs but missing filenames."""
        df = pd.DataFrame({
            self.filename_col: [None, "image2.jpg", None, "image4.jpg"],
            self.url_col: ["http://url1.com", None, "http://url3.com", "http://url4.com"]
        })
        with patch("builtins.input", return_value="n") as mock_input:
            with self.assertRaises(SystemExit):
                handle_missing_filenames(df, self.filename_col, self.url_col)
            # Should count 2 URLs without filenames (rows 0 and 2)
            call_args = mock_input.call_args[0][0]
            self.assertIn("2", call_args)

    def test_missing_filenames_empty_dataframe(self):
        """Should not prompt if dataframe is empty."""
        df = pd.DataFrame({
            self.filename_col: [],
            self.url_col: []
        })
        with patch("builtins.input") as mock_input:
            handle_missing_filenames(df, self.filename_col, self.url_col)
            mock_input.assert_not_called()


class TestSetupExpectedColumns(unittest.TestCase):
    """Test expected columns setup."""

    def test_basic_columns_setup(self):
        """Should return expected columns with basic arguments."""
        args = MagicMock()
        args.img_name_col = "filename"
        args.url_col = "file_url"
        args.subdir_col = None

        expected_cols, subfolders = setup_expected_columns(args)

        self.assertEqual(expected_cols["filename_col"], "filename")
        self.assertEqual(expected_cols["url_col"], "file_url")
        self.assertNotIn("subfolders", expected_cols)
        self.assertIsNone(subfolders)

    def test_columns_lowercase_conversion(self):
        """Should convert column names to lowercase."""
        args = MagicMock()
        args.img_name_col = "FileName"
        args.url_col = "FileURL"
        args.subdir_col = None

        expected_cols, subfolders = setup_expected_columns(args)

        self.assertEqual(expected_cols["filename_col"], "filename")
        self.assertEqual(expected_cols["url_col"], "fileurl")

    def test_subdir_column_setup(self):
        """Should include subdir column when provided."""
        args = MagicMock()
        args.img_name_col = "filename"
        args.url_col = "file_url"
        args.subdir_col = "species"

        expected_cols, subfolders = setup_expected_columns(args)

        self.assertEqual(expected_cols["subfolders"], "species")
        self.assertEqual(subfolders, "species")

    def test_subdir_column_lowercase(self):
        """Should convert subdir column name to lowercase."""
        args = MagicMock()
        args.img_name_col = "filename"
        args.url_col = "file_url"
        args.subdir_col = "Species"

        expected_cols, subfolders = setup_expected_columns(args)

        self.assertEqual(expected_cols["subfolders"], "species")
        self.assertEqual(subfolders, "species")

    def test_custom_column_names_with_subdir(self):
        """Should handle custom column names with subdir."""
        args = MagicMock()
        args.img_name_col = "ImageID"
        args.url_col = "DownloadURL"
        args.subdir_col = "Category"

        expected_cols, subfolders = setup_expected_columns(args)

        self.assertEqual(expected_cols["filename_col"], "imageid")
        self.assertEqual(expected_cols["url_col"], "downloadurl")
        self.assertEqual(expected_cols["subfolders"], "category")
        self.assertEqual(subfolders, "category")

    def test_returns_subfolders_none_when_not_provided(self):
        """Should return None for subfolders when not provided in args."""
        args = MagicMock()
        args.img_name_col = "filename"
        args.url_col = "file_url"
        args.subdir_col = None

        expected_cols, subfolders = setup_expected_columns(args)

        self.assertIsNone(subfolders)
        self.assertNotIn("subfolders", expected_cols)

    def test_returns_subfolders_value_when_provided(self):
        """Should return subfolders value when provided."""
        args = MagicMock()
        args.img_name_col = "filename"
        args.url_col = "file_url"
        args.subdir_col = "region"

        expected_cols, subfolders = setup_expected_columns(args)

        self.assertEqual(subfolders, "region")
        self.assertIn("subfolders", expected_cols)

    def test_dictionary_structure(self):
        """Should return correctly structured dictionary."""
        args = MagicMock()
        args.img_name_col = "fname"
        args.url_col = "url"
        args.subdir_col = "folder"

        expected_cols, subfolders = setup_expected_columns(args)

        # Check dictionary has correct keys
        self.assertIn("filename_col", expected_cols)
        self.assertIn("url_col", expected_cols)
        self.assertIn("subfolders", expected_cols)

        # Check values are correct
        self.assertEqual(expected_cols["filename_col"], "fname")
        self.assertEqual(expected_cols["url_col"], "url")
        self.assertEqual(expected_cols["subfolders"], "folder")


if __name__ == "__main__":
    unittest.main()
