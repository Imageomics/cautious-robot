import unittest
import pandas as pd
import tempfile
import os
from cautiousrobot import BuddyCheck
from cautiousrobot.exceptions import EmptyDataFrameError

class TestBuddyCheck(unittest.TestCase):
    def setUp(self):
        self.buddy_check = BuddyCheck()
        self.buddy_check_filename = BuddyCheck(buddy_id='filename')
        self.buddy_check_id_col = BuddyCheck(buddy_id = "filename", buddy_col = "sha256")

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
        self.assertEqual(self.buddy_check_id_col.buddy_id, 'filename')
        self.assertEqual(self.buddy_check_id_col.buddy_col, 'sha256')

    def test_merge_on_checksum(self):
        source_df = pd.read_csv(self.img_source_file.name)
        checksum_df = pd.read_csv(self.checksum_source_file.name)

        merged_df = self.buddy_check_filename.merge_on_checksum(source_df, checksum_df, 'checksum')
        expected_df = pd.DataFrame({
            'filename_x': ['image1.jpg', 'image2.jpg', 'image3.jpg'],
            'checksum': ['abc123', 'def456', 'ghi789'],
            'filename_y': ['image1.jpg', 'image2.jpg', 'image3.jpg'],
            'md5': ['abc123', 'def456', 'ghi789']
        })
        pd.testing.assert_frame_equal(merged_df, expected_df)

    def test_merge_on_filename_checksum(self):
        source_df = pd.read_csv(self.img_source_file.name)
        checksum_df = pd.read_csv(self.checksum_source_file.name)
        merged_df = self.buddy_check_filename.merge_on_filename_checksum(source_df, checksum_df, 'filename', 'checksum')
        expected_df = pd.DataFrame({
            'filename': ['image1.jpg', 'image2.jpg', 'image3.jpg'],
            'checksum': ['abc123', 'def456', 'ghi789'],
            'md5': ['abc123', 'def456', 'ghi789']
        })
        pd.testing.assert_frame_equal(merged_df, expected_df)

    def test_check_alignment_all_matching(self):
        source_df = pd.read_csv(self.img_source_file.name)
        checksum_df = pd.read_csv(self.checksum_source_file.name)
        merged_df = self.buddy_check_filename.merge_on_filename_checksum(source_df, checksum_df, 'filename', 'checksum')
        missing_imgs = self.buddy_check_filename.check_alignment(source_df, merged_df)
        self.assertIsNone(missing_imgs)

    def test_check_alignment_some_missing(self):
        source_df = pd.DataFrame({
            'filename': ['image1.jpg', 'image2.jpg', 'image3.jpg', 'image4.jpg'],
            'checksum': ['abc123', 'def456', 'ghi789', 'jkl012']
        })
        checksum_df = pd.read_csv(self.checksum_source_file.name)
        merged_df = self.buddy_check_filename.merge_on_filename_checksum(source_df, checksum_df, 'filename', 'checksum')
        missing_imgs = self.buddy_check_filename.check_alignment(source_df, merged_df)
        expected_missing_imgs = pd.DataFrame({
            'filename': ['image4.jpg'],
            'checksum': ['jkl012']
        })
        pd.testing.assert_frame_equal(missing_imgs.reset_index(drop=True), expected_missing_imgs)

    def test_validate_download_success(self):
        missing_imgs = self.buddy_check.validate_download(
            source_df=pd.read_csv(self.img_source_file.name),
            checksum_df=pd.read_csv(self.checksum_source_file.name),
            source_id_col="filename",
            source_validation_col="checksum"
        )
        self.assertIsNone(missing_imgs)

    def test_validate_download_missing_images(self):
        source_df = pd.DataFrame({
            'filename': ['image1.jpg', 'image2.jpg', 'image3.jpg', 'image4.jpg'],
            'checksum': ['abc123', 'def456', 'ghi789', 'jkl012']
        })
        checksum_df = pd.read_csv(self.checksum_source_file.name)
        missing_imgs = self.buddy_check_filename.validate_download(
            source_df=source_df,
            checksum_df=checksum_df,
            source_id_col="filename",
            source_validation_col="checksum"
        )
        expected_missing_imgs = pd.DataFrame({
            'filename': ['image4.jpg'],
            'checksum': ['jkl012']
        })
        pd.testing.assert_frame_equal(missing_imgs.reset_index(drop=True), expected_missing_imgs)

    def test_check_alignment_no_matching(self):
        source_df = pd.read_csv(self.img_source_file.name)
        checksum_df = pd.DataFrame({
            'filename': ['image4.jpg', 'image5.jpg', 'image6.jpg'],
            'md5': ['xyz123', 'uvw456', 'rst789']
        })
        merged_df = self.buddy_check_filename.merge_on_filename_checksum(source_df, checksum_df, 'filename', 'checksum')
        missing_imgs = self.buddy_check_filename.check_alignment(source_df, merged_df)
        self.assertIsNotNone(missing_imgs)
        self.assertEqual(missing_imgs.shape[0], 3)

    def test_check_alignment_checksums_only(self):
        source_df = pd.read_csv(self.img_source_file.name)
        checksum_df = pd.read_csv(self.checksum_source_file.name)
        merged_df = self.buddy_check.merge_on_checksum(source_df, checksum_df, 'checksum')
        missing_imgs = self.buddy_check.check_alignment(source_df, merged_df)
        self.assertIsNone(missing_imgs)

    def test_validate_download_empty_img_df(self):
        source_df = pd.DataFrame(columns=['filename', 'checksum'])
        checksum_df = pd.read_csv(self.checksum_source_file.name)
        with self.assertRaises(EmptyDataFrameError):
            _ = self.buddy_check.validate_download(source_df, checksum_df, 'filename', 'checksum')

    def test_validate_download_empty_checksum_df(self):
        source_df = pd.read_csv(self.img_source_file.name)
        checksum_df = pd.DataFrame(columns=['filename', 'md5'])
        with self.assertRaises(EmptyDataFrameError):
            _ = self.buddy_check_filename.validate_download(source_df, checksum_df, 'filename', 'checksum')


if __name__ == '__main__':
    unittest.main()
