> **Validation module** `**rollcall**` 
Checking for pre-download requirements to ensure that the download takes place without any interruptions or failure because of the following conditions:
- Invalid csv extension
- Duplicate filenames
- Missing filenames
- Existing images on drive
- Duplicate checksums

> **Image Processing module** `**download**` 
Downloading of images according csv provided by user and initial rollcall checks.

> **Verification module** `**buddy_check**` 
Post-download verification of the downloaded images including checks for checksums and filenames.
