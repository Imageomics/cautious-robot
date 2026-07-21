# __Current Design__

### Modularization 
> **Validation module** `rollcall` 
Checking for pre-download requirements to ensure that the download takes place without any interruptions or failure because of the following conditions:
- Invalid csv extension
- Duplicate filenames
- Missing filenames
- Existing images on drive
- Duplicate checksums

> **Image Processing module** `download` 
Downloading of images according csv provided by user and initial rollcall checks.

> **Verification module** `buddy_check` 
Post-download verification of the downloaded images including checks for checksums and filenames.

### Functions & Contracts
`rollcall module`
- __ init __
- validate_csv_extension
- validate_filename_uniqueness
- handle_missing_filenames
- setup_expected_columns
- check_existing_images
- _preview_or_save
- check_duplicate_checksums
- print_download_summary

`download module`
- downsample_and_save

`buddy_check module`
- __ init __
- merge_on_checksum
- merge_on_filename_checksum
- process_checksums
- verify_downloads

### Architecture
The struture of the files and functions after modularization and the control and data flows can be accessed [here](https://app.eraser.io/workspace/bhfPH97OkNPUhSZl0KZV).