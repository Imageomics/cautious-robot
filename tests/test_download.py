
import unittest
from unittest.mock import patch
import pandas as pd
from cautiousrobot.__main__ import download_images

# Define constants for download call
DUMMY_DATA = pd.DataFrame(data = {"filename": ["test_file"],
                                  "file_url": ["test_url"]})
IMG_DIR = "test_dir"
LOG_FILEPATH = "test_log_path"
ERROR_LOG_FILEPATH = "test_error_log_path"


class TestDownload(unittest.TestCase):
    @patch('requests.get')
    def test_response_exception(self, get_mock):
        get_mock.side_effect = Exception
        download_images(DUMMY_DATA, img_dir = IMG_DIR, log_filepath = LOG_FILEPATH,
                        error_log_filepath = ERROR_LOG_FILEPATH)

if __name__ == "__main__":
    unittest.main()