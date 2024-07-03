
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import os
import shutil
from io import BytesIO
from PIL import Image
import requests
from cautiousrobot.__main__ import download_images

DUMMY_DATA = pd.DataFrame(data = {
    "filename": ["test_file1", "test_file2"],
    "file_url": ["http://test_url1.com/image.jpg", "http://test_url2.com/image.jpg"],
    "subfolder": ["test_subfolder1", "test_subfolder2"]
})
IMG_DIR = "test_dir"
LOG_FILEPATH = "test_log_path"
ERROR_LOG_FILEPATH = "test_error_log_path"
DOWNSAMPLE_PATH = "test_downsample_dir"
DOWNSAMPLE_SIZE = 100


class TestDownload(unittest.TestCase):
    def setUp(self):
        if not os.path.exists(IMG_DIR):
            os.makedirs(IMG_DIR)
        if not os.path.exists(DOWNSAMPLE_PATH):
            os.makedirs(DOWNSAMPLE_PATH)

    def tearDown(self):
        if os.path.exists(IMG_DIR):
            shutil.rmtree(IMG_DIR)
        if os.path.exists(DOWNSAMPLE_PATH):
            shutil.rmtree(DOWNSAMPLE_PATH)
    
    @patch('requests.get')
    def test_response_exception(self, get_mock):
        get_mock.side_effect = Exception
        download_images(DUMMY_DATA, img_dir = IMG_DIR, log_filepath = LOG_FILEPATH,
                        error_log_filepath = ERROR_LOG_FILEPATH)
        
    
    @patch('requests.get')
    def test_successful_download(self, get_mock):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"fake_image_data"
        mock_response.raw = BytesIO(b"fake_image_data")
        get_mock.return_value = mock_response

        download_images(DUMMY_DATA, IMG_DIR, LOG_FILEPATH, ERROR_LOG_FILEPATH)

        for filename in DUMMY_DATA['filename']:
            self.assertTrue(os.path.isfile(f"{IMG_DIR}/{filename}"))

    # Mock Two Failed responses followed by A Successful response, ensuring that the file is downloaded after retries.
    def retry_test_template(self, get_mock, status_code):
        mock_response_retry = MagicMock()
        mock_response_retry.status_code = status_code

        mock_response_success = MagicMock()
        mock_response_success.status_code = 200
        mock_response_success.content = b"fake_image_data"
        mock_response_success.raw = BytesIO(b"fake_image_data")

        get_mock.side_effect = [mock_response_retry] * 2 + [mock_response_success]

        download_images(DUMMY_DATA, IMG_DIR, LOG_FILEPATH, ERROR_LOG_FILEPATH, retry=3)

        for filename in DUMMY_DATA['filename']:
            self.assertTrue(os.path.isfile(f"{IMG_DIR}/{filename}"))

    # 429 Too Many Requests
    @patch('requests.get')
    def test_retry_429(self, get_mock):
        self.retry_test_template(get_mock, 429)

    # 500 Internal Server Error
    @patch('requests.get')
    def test_retry_500(self, get_mock):
        self.retry_test_template(get_mock, 500)

    # 502 Bad Gateway
    @patch('requests.get')
    def test_retry_502(self, get_mock):
        self.retry_test_template(get_mock, 502)

    # 503 Service Unavailable
    @patch('requests.get')
    def test_retry_503(self, get_mock):
        self.retry_test_template(get_mock, 503)

    # 504 Gateway Timeout
    @patch('requests.get')
    def test_retry_504(self, get_mock):
        self.retry_test_template(get_mock, 504)

    # Mock Continuous Failures for the maximum number of retries, ensuring that the file is not downloaded and the error is logged.
    def failure_test_template(self, get_mock, status_code):
        mock_response_retry = MagicMock()
        mock_response_retry.status_code = status_code

        get_mock.side_effect = [mock_response_retry] * 5

        download_images(DUMMY_DATA, IMG_DIR, LOG_FILEPATH, ERROR_LOG_FILEPATH, retry=5)

        for filename in DUMMY_DATA['filename']:
            self.assertFalse(os.path.isfile(f"{IMG_DIR}/{filename}"))

    @patch('requests.get')
    def test_failure_after_retries_429(self, get_mock):
        self.failure_test_template(get_mock, 429)

    @patch('requests.get')
    def test_failure_after_retries_500(self, get_mock):
        self.failure_test_template(get_mock, 500)


    @patch('requests.get')
    def test_failed_download(self, get_mock):
        get_mock.side_effect = requests.exceptions.RequestException

        download_images(DUMMY_DATA, IMG_DIR, LOG_FILEPATH, ERROR_LOG_FILEPATH, retry=2)

        for filename in DUMMY_DATA['filename']:
            self.assertFalse(os.path.isfile(f"{IMG_DIR}/{filename}"))


    @patch('requests.get')
    @patch('PIL.Image.open')
    def test_downsampled_image_creation(self, open_mock, get_mock):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"fake_image_data"
        mock_response.raw = BytesIO(b"fake_image_data")
        get_mock.return_value = mock_response

        img_mock = MagicMock()
        open_mock.return_value = img_mock

        download_images(DUMMY_DATA, IMG_DIR, LOG_FILEPATH, ERROR_LOG_FILEPATH,
                        downsample_path=DOWNSAMPLE_PATH, downsample=DOWNSAMPLE_SIZE)

        for filename in DUMMY_DATA['filename']:
            self.assertTrue(os.path.isfile(f"{DOWNSAMPLE_PATH}/test_subfolder1/{filename}") or
                            os.path.isfile(f"{DOWNSAMPLE_PATH}/test_subfolder2/{filename}"))

    @patch('requests.get')
    def test_logging(self, get_mock):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"fake_image_data"
        mock_response.raw = BytesIO(b"fake_image_data")
        get_mock.return_value = mock_response

        download_images(DUMMY_DATA, IMG_DIR, LOG_FILEPATH, ERROR_LOG_FILEPATH)

        self.assertTrue(os.path.isfile(LOG_FILEPATH))
        self.assertTrue(os.path.isfile(ERROR_LOG_FILEPATH))

if __name__ == "__main__":
    unittest.main()