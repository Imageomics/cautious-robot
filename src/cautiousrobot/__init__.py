from cautiousrobot.__main__ import download_images
from cautiousrobot.buddy_check import BuddyCheck


# Create instance of the class
buddy_check_instance = BuddyCheck()

# Expose instance methods
buddy_check_instance.validate_download
buddy_check_instance.check_alignment

__all__ = ["download_images", "validate_download", "check_alignment"]
