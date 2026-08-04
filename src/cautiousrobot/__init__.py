from cautiousrobot.buddy_check import BuddyCheck
from cautiousrobot.download import download_images
from cautiousrobot.utils import downsample_and_save_image

# Create instance of the class
buddy_check_instance = BuddyCheck()

# Expose instance methods
_ = buddy_check_instance.validate_download
_ = buddy_check_instance.check_alignment

__all__ = ["check_alignment", "download_images", "downsample_and_save_image", "validate_download"]
