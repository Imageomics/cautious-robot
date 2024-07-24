import unittest
from unittest.mock import patch, MagicMock, mock_open, call
import pandas as pd
import os
import io
import shutil
from io import BytesIO
from PIL import Image
import requests
import base64
from cautiousrobot.__main__ import download_images, main

class TestDownload(unittest.TestCase):
    def setUp(self):
        self.DUMMY_DATA = pd.DataFrame(data={
            "filename": ["test_file1", "test_file2"],
            "file_url": ["http://test_url1.com/image.jpg", "http://test_url2.com/image.jpg"],
            "subfolder": ["test_subfolder1", "test_subfolder2"]
        })
        self.IMG_DIR = "test_dir"
        self.LOG_FILEPATH = "test_log_path.jsonl"
        self.ERROR_LOG_FILEPATH = "test_error_log_path.jsonl"
        self.DOWNSAMPLE_PATH = "test_downsample_dir"
        self.DOWNSAMPLE_SIZE = 100

        os.makedirs(self.IMG_DIR, exist_ok=True)
        os.makedirs(self.DOWNSAMPLE_PATH, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.IMG_DIR, ignore_errors=True)
        shutil.rmtree(self.DOWNSAMPLE_PATH, ignore_errors=True)
        if os.path.exists(self.LOG_FILEPATH):
            os.remove(self.LOG_FILEPATH)
        if os.path.exists(self.ERROR_LOG_FILEPATH):
            os.remove(self.ERROR_LOG_FILEPATH)

    @patch('requests.get')
    def test_response_exception(self, get_mock):
        get_mock.side_effect = requests.exceptions.RequestException
        download_images(self.DUMMY_DATA, self.IMG_DIR, self.LOG_FILEPATH, self.ERROR_LOG_FILEPATH)
        for filename in self.DUMMY_DATA['filename']:
            self.assertFalse(os.path.isfile(f"{self.IMG_DIR}/{filename}"))

    @patch('requests.get')
    def test_successful_download(self, get_mock):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raw = BytesIO(b"fake_image_data")
        get_mock.return_value = mock_response

        download_images(self.DUMMY_DATA, self.IMG_DIR, self.LOG_FILEPATH, self.ERROR_LOG_FILEPATH)

        for filename in self.DUMMY_DATA['filename']:
            self.assertTrue(os.path.isfile(f"{self.IMG_DIR}/{filename}"))

    def retry_test_template(self, get_mock, status_code):
        mock_response_retry = MagicMock()
        mock_response_retry.status_code = status_code
        mock_response_success = MagicMock()
        mock_response_success.status_code = 200
        mock_response_success.raw = BytesIO(b"fake_image_data")
        get_mock.side_effect = [mock_response_retry] * 2 + [mock_response_success]

        download_images(self.DUMMY_DATA, self.IMG_DIR, self.LOG_FILEPATH, self.ERROR_LOG_FILEPATH, retry=3)

        for filename in self.DUMMY_DATA['filename']:
            self.assertTrue(os.path.isfile(f"{self.IMG_DIR}/{filename}"))

    @patch('requests.get')
    def test_retry_429(self, get_mock):
        self.retry_test_template(get_mock, 429)

    @patch('requests.get')
    def test_retry_500(self, get_mock):
        self.retry_test_template(get_mock, 500)

    @patch('requests.get')
    def test_retry_502(self, get_mock):
        self.retry_test_template(get_mock, 502)

    @patch('requests.get')
    def test_retry_503(self, get_mock):
        self.retry_test_template(get_mock, 503)

    @patch('requests.get')
    def test_retry_504(self, get_mock):
        self.retry_test_template(get_mock, 504)

    def failure_test_template(self, get_mock, status_code):
        mock_response_retry = MagicMock()
        mock_response_retry.status_code = status_code
        get_mock.side_effect = [mock_response_retry] * 5

        download_images(self.DUMMY_DATA, self.IMG_DIR, self.LOG_FILEPATH, self.ERROR_LOG_FILEPATH, retry=5)

        for filename in self.DUMMY_DATA['filename']:
            self.assertFalse(os.path.isfile(f"{self.IMG_DIR}/{filename}"))

    @patch('requests.get')
    def test_failure_after_retries_429(self, get_mock):
        self.failure_test_template(get_mock, 429)

    @patch('requests.get')
    def test_failure_after_retries_500(self, get_mock):
        self.failure_test_template(get_mock, 500)

    @patch('requests.get')
    @patch('PIL.Image.open')
    def test_downsampled_image_creation(self, open_mock, get_mock):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raw = BytesIO(b"fake_image_data")
        get_mock.return_value = mock_response

        img_mock = MagicMock()
        open_mock.return_value = img_mock

        download_images(self.DUMMY_DATA, self.IMG_DIR, self.LOG_FILEPATH, self.ERROR_LOG_FILEPATH,
                        downsample_path=self.DOWNSAMPLE_PATH, downsample=self.DOWNSAMPLE_SIZE)

        for filename in self.DUMMY_DATA['filename']:
            self.assertTrue(os.path.isfile(f"{self.DOWNSAMPLE_PATH}/test_subfolder1/{filename}") or
                            os.path.isfile(f"{self.DOWNSAMPLE_PATH}/test_subfolder2/{filename}"))    

    @patch('requests.get')
    def test_logging(self, get_mock):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raw = BytesIO(b"fake_image_data")
        get_mock.return_value = mock_response

        download_images(self.DUMMY_DATA, self.IMG_DIR, self.LOG_FILEPATH, self.ERROR_LOG_FILEPATH)

        self.assertTrue(os.path.isfile(self.LOG_FILEPATH))
        self.assertTrue(os.path.isfile(self.ERROR_LOG_FILEPATH))
    
class TestMainFunction(unittest.TestCase):
    @patch('cautiousrobot.__main__.parse_args')
    @patch('cautiousrobot.__main__.process_csv')
    @patch('cautiousrobot.__main__.download_images')
    @patch('cautiousrobot.__main__.get_checksums')
    @patch('cautiousrobot.__main__.BuddyCheck')
    @patch('os.path.exists')
    @patch('builtins.input', return_value='y')
    def test_main_successful_execution(self, mock_input, mock_exists, mock_BuddyCheck, mock_get_checksums, mock_download_images, mock_process_csv, mock_parse_args):
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
    def test_main_csv_extension_error(self, mock_parse_args):
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