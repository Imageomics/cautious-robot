"""
Unit tests for BuddyCheck checksum helpers.

Tests checksum processing and download verification functionality.
"""

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import tempfile
import os
from argparse import Namespace
from cautiousrobot.buddy_check import process_checksums, verify_downloads


class TestProcessChecksums(unittest.TestCase):
    """Test cases for the process_checksums function."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.img_dir = os.path.join(self.temp_dir, "images")
        os.makedirs(self.img_dir)
        self.metadata_path = os.path.join(self.temp_dir, "metadata")

        # Create test args object
        self.args = Namespace(
            checksum_algorithm="md5",
            input_file="test.csv"
        )

        # Create sample source DataFrame
        self.source_df = pd.DataFrame({
            "filename": ["image1.jpg", "image2.png"],
            "file_url": ["http://example.com/img1.jpg", "http://example.com/img2.png"]
        })

    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir)

    @patch('cautiousrobot.buddy_check.get_checksums')
    def test_process_checksums_success(self, mock_get_checksums):
        """Test successful checksum processing."""
        # Create a temporary checksum file
        checksum_path = self.metadata_path + "_checksums.csv"
        checksum_df = pd.DataFrame({
            "filename": ["image1.jpg", "image2.png"],
            "md5": ["abc123", "def456"]
        })
        checksum_df.to_csv(checksum_path, index=False)

        # Call the function
        result_df, expected_num = process_checksums(
            self.img_dir, self.metadata_path, self.args, self.source_df
        )

        # Assertions
        mock_get_checksums.assert_called_once_with(
            input_path=self.img_dir,
            output_filepath=checksum_path,
            algorithm="md5"
        )
        self.assertIsNotNone(result_df)
        self.assertEqual(expected_num, 2)
        pd.testing.assert_frame_equal(result_df, checksum_df)

    @patch('cautiousrobot.buddy_check.get_checksums')
    def test_process_checksums_failure(self, mock_get_checksums):
        """Test checksum processing when get_checksums fails."""
        mock_get_checksums.side_effect = Exception("Checksum calculation failed")

        with patch('builtins.print') as mock_print:
            result_df, expected_num = process_checksums(
                self.img_dir, self.metadata_path, self.args, self.source_df
            )

        # Assertions
        self.assertIsNone(result_df)
        self.assertIsNone(expected_num)
        mock_print.assert_any_call(
            "checksum calculation of downloaded images was unsuccessful due to "
            "Checksum calculation failed."
        )

    @patch('cautiousrobot.buddy_check.get_checksums')
    def test_process_checksums_prints_count_message(self, mock_get_checksums):
        """Test that process_checksums prints the count message."""
        checksum_path = self.metadata_path + "_checksums.csv"
        checksum_df = pd.DataFrame({
            "filename": ["image1.jpg", "image2.png"],
            "md5": ["abc123", "def456"]
        })
        checksum_df.to_csv(checksum_path, index=False)

        with patch('builtins.print') as mock_print:
            process_checksums(
                self.img_dir, self.metadata_path, self.args, self.source_df
            )

        # Check that the count message was printed
        printed_messages = [str(call) for call in mock_print.call_args_list]
        self.assertTrue(
            any("There are 2 files in" in str(call) for call in printed_messages)
        )

        
    @patch('cautiousrobot.buddy_check.get_checksums')
    def test_process_checksums_mismatched_counts(self, mock_get_checksums):
        """Test process_checksums when checksum count differs from source count."""
        checksum_path = self.metadata_path + "_checksums.csv"
        checksum_df = pd.DataFrame({
            "filename": ["image1.jpg"],
            "md5": ["abc123"]
        })
        checksum_df.to_csv(checksum_path, index=False)

        source_df = pd.DataFrame({
            "filename": ["image1.jpg", "image2.png"],
            "file_url": ["http://example.com/img1.jpg", "http://example.com/img2.png"]
        })

        result_df, expected_num = process_checksums(
            self.img_dir, self.metadata_path, self.args, source_df
        )

        # Should still return the dataframes even with mismatched counts
        self.assertIsNotNone(result_df)
        self.assertEqual(expected_num, 2)


class TestVerifyDownloads(unittest.TestCase):
    """Test cases for the verify_downloads function."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.metadata_path = os.path.join(self.temp_dir, "metadata")

        # Create test args object
        self.args = Namespace(
            verifier_col=None,
            checksum_algorithm="md5",
            input_file="test.csv"
        )

        # Create sample DataFrames
        self.source_df = pd.DataFrame({
            "filename": ["image1.jpg", "image2.png"],
            "file_url": ["http://example.com/img1.jpg", "http://example.com/img2.png"],
            "md5": ["abc123", "def456"]
        })

        self.checksum_df = pd.DataFrame({
            "filename": ["image1.jpg", "image2.png"],
            "md5": ["abc123", "def456"]
        })

        self.filename_col = "filename"
        self.expected_num_imgs = 2

    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_verify_downloads_no_verifier_col(self):
        """Test verify_downloads when verifier_col is None."""
        self.args.verifier_col = None

        with patch('builtins.print') as mock_print:
            verify_downloads(
                self.args,
                self.source_df,
                self.checksum_df,
                self.filename_col,
                self.metadata_path,
                self.expected_num_imgs
            )

        # Should not print anything when verifier_col is None
        mock_print.assert_not_called()

    @patch('cautiousrobot.buddy_check.BuddyCheck')
    def test_verify_downloads_success_no_missing(self, mock_buddy_check_class):
        self.args.verifier_col = "md5"

        mock_buddy_check = MagicMock()
        mock_buddy_check_class.return_value = mock_buddy_check
        mock_buddy_check.validate_download.return_value = None

        with patch('builtins.print') as mock_print:
            verify_downloads(
                self.args,
                self.source_df,
                self.checksum_df,
                self.filename_col,
                self.metadata_path,
                self.expected_num_imgs
            )

        # Assertions
        mock_buddy_check_class.assert_called_once_with(
            buddy_id="filename", buddy_col="md5"
        )
        mock_buddy_check.validate_download.assert_called_once()
        printed_messages = [str(call) for call in mock_print.call_args_list]
        self.assertTrue(
            any("Buddy check successful" in str(call) for call in printed_messages)
        )

    @patch('cautiousrobot.buddy_check.BuddyCheck')
    def test_verify_downloads_with_missing_images(self, mock_buddy_check_class):
        self.args.verifier_col = "md5"

        # Create a missing images DataFrame
        missing_df = pd.DataFrame({
            "filename": ["image3.jpg"],
            "md5": ["fakehash3"]
        })

        mock_buddy_check = MagicMock()
        mock_buddy_check_class.return_value = mock_buddy_check
        mock_buddy_check.validate_download.return_value = missing_df

        with patch('builtins.print') as mock_print:
            with patch.object(pd.DataFrame, 'to_csv'):
                verify_downloads(
                    self.args,
                    self.source_df,
                    self.checksum_df,
                    self.filename_col,
                    self.metadata_path,
                    self.expected_num_imgs
                )

        # Assertions
        mock_buddy_check.validate_download.assert_called_once()
        printed_messages = [str(call) for call in mock_print.call_args_list]
        self.assertTrue(
            any("_missing.csv" in str(call) for call in printed_messages)
        )

    @patch('cautiousrobot.buddy_check.BuddyCheck')
    def test_verify_downloads_exception_handling(self, mock_buddy_check_class):
        self.args.verifier_col = "md5"

        mock_buddy_check = MagicMock()
        mock_buddy_check_class.return_value = mock_buddy_check
        test_exception = ValueError("Test validation error")
        mock_buddy_check.validate_download.side_effect = test_exception

        with patch('builtins.print') as mock_print:
            verify_downloads(
                self.args,
                self.source_df,
                self.checksum_df,
                self.filename_col,
                self.metadata_path,
                self.expected_num_imgs
            )

        # Assertions
        printed_messages = [str(call) for call in mock_print.call_args_list]
        self.assertTrue(
            any(
                "Verification of download failed due to ValueError: Test validation error." in str(call)
                for call in printed_messages
            )
        )

    @patch('cautiousrobot.buddy_check.BuddyCheck')
    def test_verify_downloads_uses_correct_algorithm(self, mock_buddy_check_class):
        self.args.verifier_col = "hash"
        self.args.checksum_algorithm = "sha256"

        # Use fixtures that match the requested verifier/checksum columns.
        source_df = self.source_df.copy()
        source_df["hash"] = ["abc123sha", "def456sha"]
        checksum_df = pd.DataFrame({
            "filename": ["image1.jpg", "image2.png"],
            "sha256": ["abc123sha", "def456sha"],
        })

        mock_buddy_check = MagicMock()
        mock_buddy_check_class.return_value = mock_buddy_check
        mock_buddy_check.validate_download.return_value = None

        verify_downloads(
            self.args,
            source_df,
            checksum_df,
            self.filename_col,
            self.metadata_path,
            self.expected_num_imgs
        )

        # Check that BuddyCheck was initialized with sha256 and the source checksum column was passed through.
        mock_buddy_check_class.assert_called_once_with(
            buddy_id=self.filename_col,
            buddy_col="sha256"
        )
        call_kwargs = mock_buddy_check.validate_download.call_args[1]
        self.assertEqual(call_kwargs["source_validation_col"], "hash")

    @patch('cautiousrobot.buddy_check.BuddyCheck')
    def test_verify_downloads_with_custom_filename_col(self, mock_buddy_check_class):
        """Test verify_downloads works when filename_col is not 'filename'."""
        self.args.verifier_col = "md5"
        custom_filename_col = "img_name"

        source_df = pd.DataFrame({
            custom_filename_col: ["image1.jpg", "image2.png"],
            "file_url": ["http://example.com/img1.jpg", "http://example.com/img2.png"],
            "md5": ["abc123", "def456"]
        })

        mock_buddy_check = MagicMock()
        mock_buddy_check_class.return_value = mock_buddy_check
        mock_buddy_check.validate_download.return_value = None

        verify_downloads(
            self.args,
            source_df,
            self.checksum_df,
            custom_filename_col,
            self.metadata_path,
            self.expected_num_imgs
        )

        mock_buddy_check_class.assert_called_once_with(
            buddy_id="filename",
            buddy_col="md5"
        )
        call_kwargs = mock_buddy_check.validate_download.call_args[1]
        self.assertEqual(call_kwargs["source_id_col"], custom_filename_col)
        self.assertEqual(call_kwargs["source_validation_col"], "md5")


class TestProcessChecksumsEdgeCases(unittest.TestCase):
    """Test edge cases for process_checksums."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.img_dir = os.path.join(self.temp_dir, "images")
        os.makedirs(self.img_dir)
        self.metadata_path = os.path.join(self.temp_dir, "metadata")

        self.args = Namespace(
            checksum_algorithm="md5",
            input_file="test.csv"
        )

    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir)

if __name__ == "__main__":
    unittest.main()
