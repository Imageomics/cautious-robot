import unittest
from unittest.mock import patch, MagicMock, call
import pandas as pd
from io import StringIO
from cautiousrobot.__main__ import main

class TestMainFunction(unittest.TestCase):

    @patch('cautiousrobot.__main__.parse_args')
    @patch('cautiousrobot.__main__.process_csv')
    @patch('cautiousrobot.__main__.download_images')
    @patch('cautiousrobot.__main__.get_checksums')
    @patch('cautiousrobot.__main__.BuddyCheck')
    @patch('os.path.exists')
    def test_main_successful_execution(self, mock_exists, mock_BuddyCheck, mock_get_checksums, mock_download_images, mock_process_csv, mock_parse_args):
        mock_args = MagicMock()
        mock_args.input_file = 'test.csv'
        mock_args.img_name_col = 'filename_col'
        mock_args.url_col = 'url_col'
        mock_args.subdir_col = None
        mock_args.output_dir = 'output_dir'
        mock_args.side_length = None
        mock_args.wait_time = 0
        mock_args.max_retries = 3
        mock_args.starting_idx = 0
        mock_args.checksum_algorithm = 'md5'
        mock_args.verifier_col = None

        mock_parse_args.return_value = mock_args
        mock_exists.return_value = False

        mock_data = pd.DataFrame({
            'filename_col': ['file1', 'file2', 'file3', 'file4'],
            'url_col': ['url1', 'url2', 'url3', 'url4']
        })
        
        mock_process_csv.return_value = mock_data

        try:
            main()
        except SystemExit as e:
            self.fail(f"main() raised SystemExit unexpectedly: {e}")

    @patch('cautiousrobot.__main__.parse_args')
    @patch('cautiousrobot.__main__.process_csv')
    def test_main_csv_extension_error(self, mock_process_csv, mock_parse_args):
        mock_args = MagicMock()
        mock_args.input_file = 'test.txt'
        mock_parse_args.return_value = mock_args

        with self.assertRaises(SystemExit) as cm:
            main()
        
        self.assertEqual(cm.exception.code, "Expected CSV for input file; extension should be '.csv'")

    @patch('cautiousrobot.__main__.parse_args')
    @patch('cautiousrobot.__main__.process_csv')
    def test_main_missing_columns_error(self, mock_process_csv, mock_parse_args):
        mock_args = MagicMock()
        mock_args.input_file = 'test.csv'
        mock_args.img_name_col = 'filename_col'
        mock_args.url_col = 'url_col'
        mock_args.subdir_col = None
        mock_parse_args.return_value = mock_args

        mock_process_csv.side_effect = Exception("Missing required columns")
        
        with self.assertRaises(SystemExit) as cm:
            main()
        
        self.assertEqual(cm.exception.code, "Missing required columns Please adjust inputs and try again.")

    @patch('cautiousrobot.__main__.parse_args')
    @patch('cautiousrobot.__main__.process_csv')
    def test_main_non_unique_filenames(self, mock_process_csv, mock_parse_args):
        mock_args = MagicMock()
        mock_args.input_file = 'test.csv'
        mock_args.img_name_col = 'filename_col'
        mock_args.url_col = 'url_col'
        mock_args.subdir_col = None
        mock_parse_args.return_value = mock_args

        mock_data = pd.DataFrame({
            'filename_col': ['file1', 'file2', 'file1', 'file4'],
            'url_col': ['url1', 'url2', 'url3', 'url4']
        })
        
        mock_process_csv.return_value = mock_data

        with self.assertRaises(SystemExit) as cm:
            main()
        
        self.assertEqual(
            str(cm.exception),
            "filename_col is not a unique identifier for this dataset, please choose a column with unique values for filenames."
        )

    @patch('cautiousrobot.__main__.parse_args')
    @patch('cautiousrobot.__main__.process_csv')
    @patch('builtins.input', return_value='n')
    def test_main_missing_filenames(self, mock_input, mock_process_csv, mock_parse_args):
        mock_args = MagicMock()
        mock_args.input_file = 'test.csv'
        mock_args.img_name_col = 'filename_col'
        mock_args.url_col = 'url_col'
        mock_args.subdir_col = None
        mock_parse_args.return_value = mock_args

        mock_data = pd.DataFrame({
            'filename_col': [None, None, 'file2', 'file3'],
            'url_col': ['url1', 'url2', 'url3', 'url4']
        })
        
        mock_process_csv.return_value = mock_data

        with self.assertRaises(SystemExit) as cm:
            main()
        
        self.assertEqual(cm.exception.code, "Exited without executing.")

    @patch('cautiousrobot.__main__.parse_args')
    @patch('cautiousrobot.__main__.process_csv')
    @patch('builtins.input', return_value='n')
    @patch('os.path.exists', return_value=True)
    def test_main_directory_exists(self, mock_exists, mock_input, mock_process_csv, mock_parse_args):
        mock_args = MagicMock()
        mock_args.input_file = 'test.csv'
        mock_args.img_name_col = 'filename_col'
        mock_args.url_col = 'url_col'
        mock_args.subdir_col = None
        mock_args.output_dir = 'output_dir'
        mock_args.side_length = None
        mock_args.wait_time = 0
        mock_args.max_retries = 3
        mock_args.starting_idx = 0
        mock_args.checksum_algorithm = 'md5'
        mock_args.verifier_col = None

        mock_parse_args.return_value = mock_args

        mock_data = pd.DataFrame({
            'filename_col': ['file1', 'file2', 'file3', 'file4'],
            'url_col': ['url1', 'url2', 'url3', 'url4']
        })
        
        mock_process_csv.return_value = mock_data

        with self.assertRaises(SystemExit) as cm:
            main()
        
        self.assertEqual(cm.exception.code, "Exited without executing.")


if __name__ == '__main__':
    unittest.main()
