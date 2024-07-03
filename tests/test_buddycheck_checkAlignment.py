import unittest
from unittest.mock import patch
import pandas as pd
import tempfile
import os
from cautiousrobot import BuddyCheck  

class TestBuddyCheck(unittest.TestCase):
    def setUp(self):
        self.buddy_check = BuddyCheck()
        
    def test_check_alignment_all_matching(self):
        img_df = pd.DataFrame({
            'filename': ['image1.jpg', 'image2.jpg', 'image3.jpg'],
            'checksum': ['abc123', 'def456', 'ghi789']
        })
        checksum_df = pd.DataFrame({
            'filename': ['image1.jpg', 'image2.jpg', 'image3.jpg'],
            'md5': ['abc123', 'def456', 'ghi789']
        })

        missing_imgs = self.buddy_check.check_alignment(img_df, checksum_df)
        self.assertIsNone(missing_imgs)

    def test_check_alignment_some_missing(self):
        img_df = pd.DataFrame({
            'filename': ['image1.jpg', 'image2.jpg', 'image3.jpg', 'image4.jpg'],
            'checksum': ['abc123', 'def456', 'ghi789', 'jkl012']
        })
        checksum_df = pd.DataFrame({
            'filename': ['image1.jpg', 'image2.jpg', 'image3.jpg'],
            'md5': ['abc123', 'def456', 'ghi789']
        })

        missing_imgs = self.buddy_check.check_alignment(img_df, checksum_df, buddy_id='filename', buddy_col='md5')
        self.assertIsNotNone(missing_imgs)
        self.assertEqual(missing_imgs.shape[0], 1)
        self.assertEqual(missing_imgs['filename'].values[0], 'image4.jpg')

    def test_check_alignment_no_matching(self):
        img_df = pd.DataFrame({
            'filename': ['image1.jpg', 'image2.jpg', 'image3.jpg'],
            'checksum': ['abc123', 'def456', 'ghi789']
        })
        checksum_df = pd.DataFrame({
            'filename': ['image4.jpg', 'image5.jpg', 'image6.jpg'],
            'md5': ['xyz123', 'uvw456', 'rst789']
        })

        missing_imgs = self.buddy_check.check_alignment(img_df, checksum_df, buddy_id='filename', buddy_col='md5')
        self.assertIsNotNone(missing_imgs)
        self.assertEqual(missing_imgs.shape[0], 3)

    def test_check_alignment_checksums_only(self):
        img_df = pd.DataFrame({
            'filename': ['image1.jpg', 'image2.jpg', 'image3.jpg'],
            'checksum': ['abc123', 'def456', 'ghi789']
        })
        checksum_df = pd.DataFrame({
            'md5': ['abc123', 'def456', 'ghi789']
        })

        missing_imgs = self.buddy_check.check_alignment(img_df, checksum_df, buddy_id='filename', buddy_col='md5')
        self.assertIsNone(missing_imgs)

    #  correctly identifies that all filenames have matching checksums in both DataFrames and that it returns None when there are no mismatches. 
    def test_check_alignment_filenames_and_checksums(self):
        img_df = pd.DataFrame({
            'filename': ['image1.jpg', 'image2.jpg', 'image3.jpg'],
            'checksum': ['abc123', 'def456', 'ghi789']
        })
        checksum_df = pd.DataFrame({
            'filename': ['image1.jpg', 'image2.jpg', 'image3.jpg'],
            'md5': ['abc123', 'def456', 'ghi789']
        })

        missing_imgs = self.buddy_check.check_alignment(img_df, checksum_df, buddy_id='filename', buddy_col='md5')
        self.assertIsNone(missing_imgs)

    def test_check_alignment_empty_img_df(self):
        img_df = pd.DataFrame(columns=['filename', 'checksum'])
        checksum_df = pd.DataFrame({
            'filename': ['image1.jpg', 'image2.jpg', 'image3.jpg'],
            'md5': ['abc123', 'def456', 'ghi789']
        })

        missing_imgs = self.buddy_check.check_alignment(img_df, checksum_df)
        self.assertIsNone(missing_imgs)

    def test_check_alignment_empty_checksum_df(self):
        img_df = pd.DataFrame({
            'filename': ['image1.jpg', 'image2.jpg', 'image3.jpg'],
            'checksum': ['abc123', 'def456', 'ghi789']
        })
        checksum_df = pd.DataFrame(columns=['filename', 'md5'])

        try:
            missing_imgs = self.buddy_check.check_alignment(img_df, checksum_df, buddy_id='filename', buddy_col='md5')
            self.assertIsNotNone(missing_imgs)
            self.assertEqual(missing_imgs.shape[0], 3)
        except KeyError as e:
            print(f"Error in test_check_alignment_empty_checksum_df: {e}")
            self.fail("Exception raised in test_check_alignment_empty_checksum_df")


    def test_check_alignment_case_insensitive_columns(self):
        img_df = pd.DataFrame({
            'FILENAME': ['image1.jpg', 'image2.jpg', 'image3.jpg'],
            'CHECKSUM': ['abc123', 'def456', 'ghi789']
        })
        checksum_df = pd.DataFrame({
            'FILENAME': ['image1.jpg', 'image2.jpg', 'image3.jpg'],
            'MD5': ['abc123', 'def456', 'ghi789']
        })

        missing_imgs = self.buddy_check.check_alignment(img_df.rename(columns=str.lower), checksum_df.rename(columns=str.lower))
        self.assertIsNone(missing_imgs)


if __name__ == '__main__':
    unittest.main()
