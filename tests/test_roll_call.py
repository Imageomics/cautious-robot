import unittest
from unittest.mock import patch, MagicMock
import pandas as pd

from cautiousrobot.roll_call import RollCall


class TestValidateCsvExtension(unittest.TestCase):
    """Test CSV file extension validation."""

    def setUp(self):
        self.rollcall = RollCall()
        
    def test_valid_csv_extension(self):
        """Should not raise exception for valid .csv file."""
        # Should not raise any exception
        try:
            self.rollcall.validate_csv_extension("data.csv")
        except SystemExit:
            self.fail("validate_csv_extension raised SystemExit unexpectedly")

    def test_csv_extension_case_sensitive(self):
        """Should accept .csv regardless of path content."""
        try:
            self.rollcall.validate_csv_extension("MyData.csv")
            self.rollcall.validate_csv_extension("path/to/data.csv")
        except SystemExit:
            self.fail("validate_csv_extension raised SystemExit unexpectedly")

    def test_invalid_txt_extension(self):
        """Should exit if file has .txt extension."""
        with self.assertRaises(SystemExit) as cm:
            self.rollcall.validate_csv_extension("data.txt")
        self.assertIn("csv", cm.exception.code.lower())

    def test_invalid_xlsx_extension(self):
        """Should exit if file has .xlsx extension."""
        with self.assertRaises(SystemExit) as cm:
            self.rollcall.validate_csv_extension("data.xlsx")
        self.assertIn("csv", cm.exception.code.lower())

    def test_invalid_no_extension(self):
        """Should exit if file has no extension."""
        with self.assertRaises(SystemExit) as cm:
            self.rollcall.validate_csv_extension("data")
        self.assertIn("csv", cm.exception.code.lower())

    def test_invalid_csv_as_substring(self):
        """Should require .csv as actual extension, not substring."""
        with self.assertRaises(SystemExit) as cm:
            self.rollcall.validate_csv_extension("data.csv.backup")
        self.assertIn("csv", cm.exception.code.lower())


class TestValidateFilenameUniqueness(unittest.TestCase):
    """Test filename uniqueness validation."""

    def setUp(self):
        self.rollcall = RollCall()

    def test_unique_filenames(self):
        """Should not raise exception for unique filenames."""
        df = pd.DataFrame({
            "filename": ["image1.jpg", "image2.jpg", "image3.jpg"]
        })
        try:
            self.rollcall.validate_filename_uniqueness(df, "filename")
        except SystemExit:
            self.fail("validate_filename_uniqueness raised SystemExit unexpectedly")

    def test_duplicate_filenames(self):
        """Should exit if filenames are not unique."""
        df = pd.DataFrame({
            "filename": ["image1.jpg", "image1.jpg", "image2.jpg"]
        })
        with self.assertRaises(SystemExit) as cm:
            self.rollcall.validate_filename_uniqueness(df, "filename")
        self.assertIn("unique identifier", cm.exception.code.lower())

    def test_filenames_with_missing_values(self):
        """Should only count non-null filenames for uniqueness."""
        df = pd.DataFrame({
            "filename": ["image1.jpg", None, "image2.jpg", None]
        })
        try:
            self.rollcall.validate_filename_uniqueness(df, "filename")
        except SystemExit:
            self.fail("validate_filename_uniqueness raised SystemExit unexpectedly")

    def test_all_missing_filenames(self):
        """Should not raise exception if all filenames are missing."""
        df = pd.DataFrame({
            "filename": [None, None, None]
        })
        try:
            self.rollcall.validate_filename_uniqueness(df, "filename")
        except SystemExit:
            self.fail("validate_filename_uniqueness raised SystemExit unexpectedly")

    def test_single_filename(self):
        """Should not raise exception for single unique filename."""
        df = pd.DataFrame({
            "filename": ["image1.jpg"]
        })
        try:
            self.rollcall.validate_filename_uniqueness(df, "filename")
        except SystemExit:
            self.fail("validate_filename_uniqueness raised SystemExit unexpectedly")

    def test_empty_dataframe(self):
        """Should not raise exception for empty dataframe."""
        df = pd.DataFrame({
            "filename": []
        })
        try:
            self.rollcall.validate_filename_uniqueness(df, "filename")
        except SystemExit:
            self.fail("validate_filename_uniqueness raised SystemExit unexpectedly")

    def test_custom_column_name(self):
        """Should work with custom column names."""
        df = pd.DataFrame({
            "image_name": ["photo1.jpg", "photo2.jpg", "photo3.jpg"]
        })
        try:
            self.rollcall.validate_filename_uniqueness(df, "image_name")
        except SystemExit:
            self.fail("validate_filename_uniqueness raised SystemExit unexpectedly")

    def test_duplicate_with_custom_column(self):
        """Should detect duplicates in custom column names."""
        df = pd.DataFrame({
            "image_id": ["img_001", "img_001", "img_002"]
        })
        with self.assertRaises(SystemExit) as cm:
            self.rollcall.validate_filename_uniqueness(df, "image_id")
        self.assertIn("unique identifier", cm.exception.code.lower())


class TestHandleMissingFilenames(unittest.TestCase):
    """Test handling of missing filename values under new non-interactive behavior."""

    def setUp(self):
        self.filename_col = "filename"
        self.url_col = "file_url"
        # RollCall now requires csv_path for saving missing CSVs
        self.rollcall = RollCall(csv_path="testdata.csv")

    @patch("builtins.print")
    def test_no_missing_filenames(self, mock_print):
        """Should not print anything when no filenames are missing."""
        df = pd.DataFrame({
            self.filename_col: ["image1.jpg", "image2.jpg"],
            self.url_col: ["http://url1.com", "http://url2.com"]
        })
        result = self.rollcall.handle_missing_filenames(df, self.filename_col, self.url_col)
        self.assertIsNone(result)
        mock_print.assert_not_called()

    @patch("builtins.print")
    def test_missing_filenames_prints_when_five_or_fewer(self, mock_print):
        """Should print missing rows when count <= 5."""
        df = pd.DataFrame({
            self.filename_col: [None, None, "img3.jpg"],
            self.url_col: ["url1", "url2", "url3"]
        })
        result = self.rollcall.handle_missing_filenames(df, self.filename_col, self.url_col)

        self.assertEqual(len(result), 2)
        mock_print.assert_called()  # printed the missing rows

    @patch("pandas.DataFrame.to_csv")
    @patch("builtins.print")
    def test_missing_filenames_saved_when_more_than_five(self, mock_print, mock_to_csv):
        """Should save missing rows to CSV when count > 5."""
        df = pd.DataFrame({
            self.filename_col: [None] * 6,
            self.url_col: ["url"] * 6
        })

        result = self.rollcall.handle_missing_filenames(df, self.filename_col, self.url_col)

        self.assertEqual(len(result), 6)
        mock_to_csv.assert_called_once()
        args, kwargs = mock_to_csv.call_args
        self.assertIn("testdata_missing_filenames.csv", args[0])

    @patch("builtins.print")
    def test_missing_filenames_no_urls(self, mock_print):
        """Should not print anything when URLs are missing (no actionable missing filenames)."""
        df = pd.DataFrame({
            self.filename_col: ["img1.jpg", None, "img3.jpg"],
            self.url_col: [None, None, None]
        })
        result = self.rollcall.handle_missing_filenames(df, self.filename_col, self.url_col)

        self.assertIsNone(result)
        mock_print.assert_not_called()

    @patch("builtins.print")
    def test_missing_filenames_empty_dataframe(self, mock_print):
        """Should not print anything for empty dataframe."""
        df = pd.DataFrame({
            self.filename_col: [],
            self.url_col: []
        })
        result = self.rollcall.handle_missing_filenames(df, self.filename_col, self.url_col)

        self.assertIsNone(result)
        mock_print.assert_not_called()

    @patch("builtins.print")
    def test_missing_filenames_returns_correct_subset(self, mock_print):
        """Should return only rows with missing filenames and valid URLs."""
        df = pd.DataFrame({
            self.filename_col: [None, "img2.jpg", None, "img4.jpg"],
            self.url_col: ["url1", None, "url3", "url4"]
        })

        result = self.rollcall.handle_missing_filenames(df, self.filename_col, self.url_col)

        # Only rows 0 and 2 qualify
        self.assertEqual(len(result), 2)
        self.assertListEqual(result.index.tolist(), [0, 2])


class TestSetupExpectedColumns(unittest.TestCase):
    """Test expected columns setup."""

    def setUp(self):
        self.rollcall = RollCall()

    def test_basic_columns_setup(self):
        """Should return expected columns with basic arguments."""
        args = MagicMock()
        args.img_name_col = "filename"
        args.url_col = "file_url"
        args.subdir_col = None

        expected_cols, subfolders = self.rollcall.setup_expected_columns(args)

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

        expected_cols, subfolders = self.rollcall.setup_expected_columns(args)

        self.assertEqual(expected_cols["filename_col"], "filename")
        self.assertEqual(expected_cols["url_col"], "fileurl")

    def test_subdir_column_setup(self):
        """Should include subdir column when provided."""
        args = MagicMock()
        args.img_name_col = "filename"
        args.url_col = "file_url"
        args.subdir_col = "species"

        expected_cols, subfolders = self.rollcall.setup_expected_columns(args)

        self.assertEqual(expected_cols["subfolders"], "species")
        self.assertEqual(subfolders, "species")

    def test_subdir_column_lowercase(self):
        """Should convert subdir column name to lowercase."""
        args = MagicMock()
        args.img_name_col = "filename"
        args.url_col = "file_url"
        args.subdir_col = "Species"

        expected_cols, subfolders = self.rollcall.setup_expected_columns(args)

        self.assertEqual(expected_cols["subfolders"], "species")
        self.assertEqual(subfolders, "species")

    def test_custom_column_names_with_subdir(self):
        """Should handle custom column names with subdir."""
        args = MagicMock()
        args.img_name_col = "ImageID"
        args.url_col = "DownloadURL"
        args.subdir_col = "Category"

        expected_cols, subfolders = self.rollcall.setup_expected_columns(args)

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

        expected_cols, subfolders = self.rollcall.setup_expected_columns(args)

        self.assertIsNone(subfolders)
        self.assertNotIn("subfolders", expected_cols)

    def test_returns_subfolders_value_when_provided(self):
        """Should return subfolders value when provided."""
        args = MagicMock()
        args.img_name_col = "filename"
        args.url_col = "file_url"
        args.subdir_col = "region"

        expected_cols, subfolders = self.rollcall.setup_expected_columns(args)

        self.assertEqual(subfolders, "region")
        self.assertIn("subfolders", expected_cols)

    def test_dictionary_structure(self):
        """Should return correctly structured dictionary."""
        args = MagicMock()
        args.img_name_col = "fname"
        args.url_col = "url"
        args.subdir_col = "folder"

        expected_cols, subfolders = self.rollcall.setup_expected_columns(args)

        # Check dictionary has correct keys
        self.assertIn("filename_col", expected_cols)
        self.assertIn("url_col", expected_cols)
        self.assertIn("subfolders", expected_cols)

        # Check values are correct
        self.assertEqual(expected_cols["filename_col"], "fname")
        self.assertEqual(expected_cols["url_col"], "url")
        self.assertEqual(expected_cols["subfolders"], "folder")

class TestCheckDuplicateChecksums(unittest.TestCase):
    """Tests for RollCall.check_duplicate_checksums."""

    def setUp(self):
        self.rollcall = RollCall(csv_path="testdata.csv")

    @patch("builtins.print")
    def test_no_duplicates(self, mock_print):
        """Should not exit when no duplicate checksums exist."""
        df = pd.DataFrame({"hash": ["a", "b", "c"]})

        try:
            self.rollcall.check_duplicate_checksums(df, "hash")
        except SystemExit:
            self.fail("Unexpected SystemExit for no duplicates")

        mock_print.assert_called_once()  # "✔ No duplicate checksums detected."

    @patch("builtins.print")
    def test_duplicates_without_ignore(self, mock_print):
        """Should exit when duplicates exist and ignore flag is False."""
        df = pd.DataFrame({"hash": ["a", "a", "b"]})

        with self.assertRaises(SystemExit):
            self.rollcall.check_duplicate_checksums(df, "hash", ignore_duplicates=False)

        # Should have printed preview/save message
        mock_print.assert_called()

    @patch("builtins.print")
    def test_duplicates_with_ignore(self, mock_print):
        """Should not exit when duplicates exist and ignore flag is True."""
        df = pd.DataFrame({"hash": ["a", "a", "b"]})

        try:
            self.rollcall.check_duplicate_checksums(df, "hash", ignore_duplicates=True)
        except SystemExit:
            self.fail("Unexpected SystemExit when ignore_duplicates=True")

        # Should print warning
        self.assertTrue(any("ignore-duplicates" in call.args[0] for call in mock_print.call_args_list))

    @patch("builtins.print")
    def test_preview_mode_for_small_duplicate_set(self, mock_print):
        """Should print preview when <=5 duplicate rows."""
        df = pd.DataFrame({"hash": ["x", "x", "x"]})

        with self.assertRaises(SystemExit):
            self.rollcall.check_duplicate_checksums(df, "hash")

        # Should print the DataFrame preview
        mock_print.assert_called()

    @patch("pandas.DataFrame.to_csv")
    @patch("builtins.print")
    def test_save_mode_for_large_duplicate_set(self, mock_print, mock_to_csv):
        """Should save CSV when >5 duplicate rows."""
        df = pd.DataFrame({"hash": ["x"] * 10})

        with self.assertRaises(SystemExit):
            self.rollcall.check_duplicate_checksums(df, "hash")

        mock_to_csv.assert_called_once()
        args, kwargs = mock_to_csv.call_args
        self.assertIn("testdata_duplicate_checksums.csv", args[0])

class TestPreviewOrSave(unittest.TestCase):
    """Tests for RollCall._preview_or_save."""

    def setUp(self):
        self.rollcall = RollCall(csv_path="testdata.csv")

    @patch("builtins.print")
    @patch("pandas.DataFrame.to_csv")
    def test_preview_small_dataframe(self, mock_to_csv, mock_print):
        """Should print when <=5 rows."""
        df = pd.DataFrame({"x": [1, 2, 3]})

        self.rollcall._preview_or_save("test_label", df)

        mock_print.assert_called()
        mock_to_csv.assert_not_called()

    @patch("builtins.print")
    @patch("pandas.DataFrame.to_csv")
    def test_save_large_dataframe(self, mock_to_csv, mock_print):
        """Should save CSV when >5 rows."""
        df = pd.DataFrame({"x": list(range(10))})

        self.rollcall._preview_or_save("test_label", df)

        mock_to_csv.assert_called_once()
        args, kwargs = mock_to_csv.call_args
        self.assertIn("testdata_test_label.csv", args[0])
        mock_print.assert_called()

class TestPrintDownloadSummary(unittest.TestCase):
    """Tests for RollCall.print_download_summary."""

    def setUp(self):
        self.rollcall = RollCall(csv_path="testdata.csv")

    @patch("builtins.print")
    def test_summary_with_downsampling_and_subfolders(self, mock_print):
        """Should print full summary including downsized path and subfolders."""
        self.rollcall.print_download_summary(
            img_dir="/images",
            downsample_dir="/images_downsized",
            subfolders="species",
            num_images=42
        )

        printed = " ".join(call.args[0] for call in mock_print.call_args_list)

        self.assertIn("Images will be downloaded to: /images", printed)
        self.assertIn("Downsampled images will be saved to: /images_downsized", printed)
        self.assertIn("Subfolders enabled: species", printed)
        self.assertIn("Images to download: 42", printed)

    @patch("builtins.print")
    def test_summary_without_downsampling(self, mock_print):
        """Should indicate that downsampling is not requested."""
        self.rollcall.print_download_summary(
            img_dir="/images",
            downsample_dir=None,
            subfolders=None,
            num_images=10
        )

        printed = " ".join(call.args[0] for call in mock_print.call_args_list)

        self.assertIn("Images will be downloaded to: /images", printed)
        self.assertIn("Downsampled images: not requested", printed)
        self.assertIn("Subfolders: none", printed)
        self.assertIn("Images to download: 10", printed)

    @patch("builtins.print")
    def test_summary_with_subfolders_only(self, mock_print):
        """Should print subfolder info even without downsampling."""
        self.rollcall.print_download_summary(
            img_dir="/images",
            downsample_dir=None,
            subfolders="category",
            num_images=5
        )

        printed = " ".join(call.args[0] for call in mock_print.call_args_list)

        self.assertIn("Subfolders enabled: category", printed)
        self.assertIn("Images to download: 5", printed)

    @patch("builtins.print")
    def test_summary_zero_images(self, mock_print):
        """Should correctly print zero image count."""
        self.rollcall.print_download_summary(
            img_dir="/images",
            downsample_dir=None,
            subfolders=None,
            num_images=0
        )

        printed = " ".join(call.args[0] for call in mock_print.call_args_list)
        self.assertIn("Images to download: 0", printed)

    @patch("builtins.print")
    def test_summary_formatting_header(self, mock_print):
        """Should print the summary header and separator."""
        self.rollcall.print_download_summary(
            img_dir="/images",
            downsample_dir=None,
            subfolders=None,
            num_images=1
        )

        printed_lines = [call.args[0] for call in mock_print.call_args_list]

        self.assertIn("📦 Download Summary", printed_lines[0])
        self.assertIn("--------------------", printed_lines[1])


if __name__ == "__main__":
    unittest.main()
