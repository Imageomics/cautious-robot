import unittest
from unittest.mock import patch, MagicMock
import os
from PIL import Image
from cautiousrobot.utils import downsample_and_save_image

class TestDownsampleAndSaveImage(unittest.TestCase):
    """Test the downsample_and_save_image function."""
    
    def setUp(self):
        self.image_dir_path = "test_images"
        self.downsample_dir_path = "downsampled_images"
        self.downsample_size = 100
        self.log_errors = {} # Dictionary to store error logs
        self.error_log_filepath = "error_log.json"
        self.file_path = "file://example.com/image.jpg"

    def tearDown(self):
        if os.path.exists(self.image_dir_path):
            os.rmdir(self.image_dir_path)
        if os.path.exists(self.downsample_dir_path):
            os.rmdir(self.downsample_dir_path)
        if os.path.exists(self.error_log_filepath):
            os.remove(self.error_log_filepath)

    def mock_log_response_side_effect(self, log_errors, index, image, file_path, response_code):
        """Helper function to mimic the behavior of log_response."""
        log_errors[index] = {'image': image, 'file_path': file_path, 'response_code': response_code}
        return log_errors

    @patch("PIL.Image.open")
    def test_downsample_and_save_image_success(self, mock_open):
        """ Test the successful downsampling and saving of an image. """ 
        
        mock_image = MagicMock(spec=Image.Image)
        mock_open.return_value = mock_image

        # The image is resized to a new image
        mock_resized_image = MagicMock(spec=Image.Image)
        mock_image.resize.return_value = mock_resized_image

        downsample_and_save_image(
            self.image_dir_path,
            "test_image.jpg",
            self.downsample_dir_path,
            self.downsample_size,
            self.log_errors,
            0,  # image_index
            self.file_path,
            self.error_log_filepath
        )
        
        mock_open.assert_called_once_with(f"{self.image_dir_path}/test_image.jpg")
        mock_image.resize.assert_called_once_with((self.downsample_size, self.downsample_size))
        mock_resized_image.save.assert_called_once_with(f"{self.downsample_dir_path}/test_image.jpg")

    @patch("os.path.exists", return_value=True)
    @patch("PIL.Image.open", side_effect=FileNotFoundError("File not found"))
    @patch("cautiousrobot.utils.log_response")
    @patch("cautiousrobot.utils.update_log")
    def test_downsample_and_save_image_file_not_found(self, mock_update_log, mock_log_response, mock_open, mock_exists):
        """ Test the behavior when the image file is not found. """
        
        mock_log_response.side_effect = self.mock_log_response_side_effect

        downsample_and_save_image(
            self.image_dir_path,
            "missing_image.jpg",
            self.downsample_dir_path,
            self.downsample_size,
            self.log_errors,
            0,  # image_index
            self.file_path,
            self.error_log_filepath
        )

        mock_open.assert_called_once_with(f"{self.image_dir_path}/missing_image.jpg")
        mock_log_response.assert_called_once_with(
            self.log_errors,
            index=0,
            image="downsized_missing_image.jpg",
            file_path=self.file_path,
            response_code="File not found"
        )
        mock_update_log.assert_called_once_with(
            log=self.log_errors,
            index=0,
            filepath=self.error_log_filepath
        )

        # Check the log error dictionary
        self.assertIn(0, self.log_errors)
        self.assertEqual(self.log_errors[0]['response_code'], "File not found")

    @patch("os.path.exists", return_value=False)
    @patch("PIL.Image.open", side_effect=Exception("Unexpected error"))
    @patch("cautiousrobot.utils.log_response")
    @patch("cautiousrobot.utils.update_log")
    def test_downsample_and_save_image_unexpected_error(self, mock_update_log, mock_log_response, mock_open, mock_exists):
        """ Test the behavior when an unexpected error occurs. """
        
        mock_log_response.side_effect = self.mock_log_response_side_effect

        downsample_and_save_image(
            self.image_dir_path,
            "test_image.jpg",
            self.downsample_dir_path,
            self.downsample_size,
            self.log_errors,
            1, 
            self.file_path,
            self.error_log_filepath
        )

        mock_open.assert_called_once_with(f"{self.image_dir_path}/test_image.jpg")
        mock_log_response.assert_called_once_with(
            self.log_errors,
            index=1,
            image="downsized_test_image.jpg",
            file_path=self.file_path,
            response_code="Unexpected error"
        )
        mock_update_log.assert_called_once_with(
            log=self.log_errors,
            index=1,
            filepath=self.error_log_filepath
        )

        self.assertIn(1, self.log_errors)
        self.assertEqual(self.log_errors[1]['response_code'], "Unexpected error")

if __name__ == "__main__":
    unittest.main()
