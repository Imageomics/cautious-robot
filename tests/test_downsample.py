import unittest
from unittest.mock import patch, MagicMock, call
import os
import tempfile
import shutil
from PIL import Image
from cautiousrobot.downsample_and_save import downsample_and_save_image

class TestDownsampleAndSaveImage(unittest.TestCase):
    """Test the downsample_and_save_image function."""
    
    def setUp(self):
        self.image_dir_path = "test_images"
        self.downsample_dir_path = "downsampled_images"
        self.downsample_size = 100
        self.log_errors = {} # Dictionary to store error logs
        self.error_log_filepath = "error_log.json"
        self.file_path = "file://example.com/image.jpg"
        
        # Create temporary directories for integration tests
        self.temp_dir = tempfile.mkdtemp()
        self.temp_image_dir = os.path.join(self.temp_dir, "test_images")
        self.temp_downsample_dir = os.path.join(self.temp_dir, "downsampled_images")
        os.makedirs(self.temp_image_dir, exist_ok=True)

    def tearDown(self):
        if os.path.exists(self.image_dir_path):
            os.rmdir(self.image_dir_path)
        if os.path.exists(self.downsample_dir_path):
            os.rmdir(self.downsample_dir_path)
        if os.path.exists(self.error_log_filepath):
            os.remove(self.error_log_filepath)
        # Clean up temporary directories
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def mock_log_response_side_effect(self, log_errors, index, image, file_path, response_code):
        """Helper function to mimic the behavior of log_response."""
        log_errors[index] = {'image': image, 'file_path': file_path, 'response_code': response_code}
        return log_errors
    
    def create_test_image(self, filepath, size=(200, 200)):
        """Create a simple test image."""
        img = Image.new('RGB', size, color='red')
        img.save(filepath)
        return filepath

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
    @patch("cautiousrobot.downsample_and_save.log_response")
    @patch("cautiousrobot.downsample_and_save.update_log")
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
    @patch("cautiousrobot.downsample_and_save.log_response")
    @patch("cautiousrobot.downsample_and_save.update_log")
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

    @patch("os.makedirs")
    @patch("os.path.exists", return_value=False)
    @patch("PIL.Image.open")
    def test_downsample_directory_creation(self, mock_image_open, mock_exists, mock_makedirs):
        """Test that the downsample directory is created when it doesn't exist."""
        
        mock_image = MagicMock(spec=Image.Image)
        mock_image_open.return_value = mock_image
        mock_resized_image = MagicMock(spec=Image.Image)
        mock_image.resize.return_value = mock_resized_image

        downsample_and_save_image(
            self.image_dir_path,
            "test_image.jpg",
            self.downsample_dir_path,
            self.downsample_size,
            self.log_errors,
            0,
            self.file_path,
            self.error_log_filepath
        )

        # Verify os.makedirs was called with the correct parameters
        mock_makedirs.assert_called_once_with(self.downsample_dir_path, exist_ok=False)
        # Verify image was still processed
        mock_image_open.assert_called_once()

    @patch("os.path.exists", return_value=True)
    @patch("PIL.Image.open", side_effect=IOError("Cannot identify image file"))
    @patch("cautiousrobot.downsample_and_save.log_response")
    @patch("cautiousrobot.downsample_and_save.update_log")
    def test_downsample_pil_ioerror(self, mock_update_log, mock_log_response, mock_open, mock_exists):
        """Test handling of PIL IOError (corrupted image)."""
        
        mock_log_response.side_effect = self.mock_log_response_side_effect

        downsample_and_save_image(
            self.image_dir_path,
            "corrupted_image.jpg",
            self.downsample_dir_path,
            self.downsample_size,
            self.log_errors,
            2,
            self.file_path,
            self.error_log_filepath
        )

        mock_log_response.assert_called_once()
        mock_update_log.assert_called_once()
        self.assertIn(2, self.log_errors)

    @patch("os.path.exists", return_value=True)
    @patch("PIL.Image.open", side_effect=OSError("Permission denied"))
    @patch("cautiousrobot.downsample_and_save.log_response")
    @patch("cautiousrobot.downsample_and_save.update_log")
    def test_downsample_permission_error(self, mock_update_log, mock_log_response, mock_open, mock_exists):
        """Test handling of permission errors."""
        
        mock_log_response.side_effect = self.mock_log_response_side_effect

        downsample_and_save_image(
            self.image_dir_path,
            "protected_image.jpg",
            self.downsample_dir_path,
            self.downsample_size,
            self.log_errors,
            3,
            self.file_path,
            self.error_log_filepath
        )

        mock_log_response.assert_called_once()
        self.assertIn(3, self.log_errors)
        self.assertEqual(self.log_errors[3]['response_code'], "Permission denied")

    @patch("PIL.Image.open")
    def test_downsample_image_naming(self, mock_image_open):
        """Test that the output image has the correct filename."""
        
        mock_image = MagicMock(spec=Image.Image)
        mock_image_open.return_value = mock_image
        mock_resized_image = MagicMock(spec=Image.Image)
        mock_image.resize.return_value = mock_resized_image
        
        image_name = "photo_2024.png"
        
        downsample_and_save_image(
            self.image_dir_path,
            image_name,
            self.downsample_dir_path,
            self.downsample_size,
            self.log_errors,
            0,
            self.file_path,
            self.error_log_filepath
        )

        # Verify the save was called with the same image name
        expected_output_path = f"{self.downsample_dir_path}/{image_name}"
        mock_resized_image.save.assert_called_once_with(expected_output_path)

    @patch("PIL.Image.open")
    def test_downsample_resize_dimensions(self, mock_image_open):
        """Test that image is resized to the correct dimensions."""
        
        mock_image = MagicMock(spec=Image.Image)
        mock_image_open.return_value = mock_image
        mock_resized_image = MagicMock(spec=Image.Image)
        mock_image.resize.return_value = mock_resized_image
        
        custom_size = 512

        downsample_and_save_image(
            self.image_dir_path,
            "test_image.jpg",
            self.downsample_dir_path,
            custom_size,
            self.log_errors,
            0,
            self.file_path,
            self.error_log_filepath
        )

        # Verify resize was called with the correct dimensions
        mock_image.resize.assert_called_once_with((custom_size, custom_size))

    def test_downsample_integration_with_real_image(self):
        """Integration test with a real image file."""
        
        # Create a test image
        test_image_path = os.path.join(self.temp_image_dir, "test.jpg")
        self.create_test_image(test_image_path, size=(200, 200))
        
        # Verify the test image was created
        self.assertTrue(os.path.exists(test_image_path))
        with Image.open(test_image_path) as original_img:
            self.assertEqual(original_img.size, (200, 200))

        # Run downsampling
        downsample_and_save_image(
            self.temp_image_dir,
            "test.jpg",
            self.temp_downsample_dir,
            100,
            self.log_errors,
            0,
            test_image_path,
            os.path.join(self.temp_dir, "error_log.jsonl")
        )

        # Verify the downsampled image was created with correct dimensions
        output_path = os.path.join(self.temp_downsample_dir, "test.jpg")
        self.assertTrue(os.path.exists(output_path), "Downsampled image was not created")
        
        with Image.open(output_path) as downsampled_img:
            self.assertEqual(downsampled_img.size, (100, 100))
        
        # Verify no errors were logged
        self.assertEqual(len(self.log_errors), 0)

    def test_downsample_multiple_images_sequential(self):
        """Test downsampling multiple images in sequence."""
        
        # Create multiple test images
        image_names = ["image1.jpg", "image2.jpg", "image3.jpg"]
        for img_name in image_names:
            test_image_path = os.path.join(self.temp_image_dir, img_name)
            self.create_test_image(test_image_path, size=(150, 150))

        # Downsample each image
        for idx, img_name in enumerate(image_names):
            downsample_and_save_image(
                self.temp_image_dir,
                img_name,
                self.temp_downsample_dir,
                75,
                self.log_errors,
                idx,
                f"file://example.com/{img_name}",
                os.path.join(self.temp_dir, "error_log.jsonl")
            )

        # Verify all downsampled images exist with correct dimensions
        for img_name in image_names:
            output_path = os.path.join(self.temp_downsample_dir, img_name)
            self.assertTrue(os.path.exists(output_path))
            
            with Image.open(output_path) as downsampled_img:
                self.assertEqual(downsampled_img.size, (75, 75))
        
        # Verify no errors
        self.assertEqual(len(self.log_errors), 0)

if __name__ == "__main__":
    unittest.main()
