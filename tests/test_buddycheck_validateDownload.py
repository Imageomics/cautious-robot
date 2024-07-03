import unittest
from unittest.mock import patch
import pandas as pd
import tempfile
import os
from cautiousrobot import BuddyCheck
from cautiousrobot.utils import process_csv

class TestBuddyCheck(unittest.TestCase):
    def setUp(self):

        self.buddy_check = BuddyCheck()
        
        self.img_source_file = tempfile.NamedTemporaryFile(delete=False, mode='w')
        self.checksum_source_file = tempfile.NamedTemporaryFile(delete=False, mode='w')

        self.img_source_file.write("""filename,checksum
                                    image1.jpg,abc123
                                    image2.jpg,def456
                                    image3.jpg,ghi789
                                    """)
        self.img_source_file.close()
        
        self.checksum_source_file.write("""filename,md5
                                    image1.jpg,abc123
                                    image2.jpg,def456
                                    image3.jpg,ghi789
                                    """)
        self.checksum_source_file.close()

    def tearDown(self):
        os.remove(self.img_source_file.name)
        os.remove(self.checksum_source_file.name)


    def test_initialization(self):
        self.assertEqual(self.buddy_check.buddy_id, None)
        self.assertEqual(self.buddy_check.buddy_col, 'md5')

    def test_validate_download_success(self):
        missing_imgs = self.buddy_check.validate_download(
            img_source=self.img_source_file.name,
            checksum_source=self.checksum_source_file.name,
            id_col="filename",
            validation_col="checksum"
        )
        
        self.assertIsNone(missing_imgs)

    def test_validate_download_missing_images(self):
        self.img_source_file = tempfile.NamedTemporaryFile(delete=False, mode='w')
        self.img_source_file.write("""filename,checksum
                                    image1.jpg,abc123
                                    image2.jpg,def456
                                    image3.jpg,ghi789
                                    image4.jpg,jkl012
                                    """)
        self.img_source_file.close()
        
        self.checksum_source_file = tempfile.NamedTemporaryFile(delete=False, mode='w')
        self.checksum_source_file.write("""filename,md5
                                    image1.jpg,abc123
                                    image2.jpg,def456
                                    image3.jpg,ghi789
                                    """)
        self.checksum_source_file.close()
        
        try:
            missing_imgs = self.buddy_check.validate_download(
                img_source=self.img_source_file.name,
                checksum_source=self.checksum_source_file.name,
                id_col="filename",
                validation_col="checksum",
                buddy_id="filename"
            )
            
        except KeyError as e:
            raise
        
        self.assertIsNotNone(missing_imgs)
        self.assertEqual(missing_imgs.shape[0], 1)
        self.assertEqual(missing_imgs['filename'].values[0], 'image4.jpg')


if __name__ == '__main__':
    unittest.main()
