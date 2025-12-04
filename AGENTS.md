# Copilot Instructions for cautious-robot

## Project Overview

**cautious-robot** is a Python CLI tool designed to download images from URLs specified in CSV files. It provides robust downloading capabilities with verification, error handling, and optional image downsampling.

### Key Features
- Download images from URLs in CSV files
- Generate and verify checksums (MD5, SHA256, etc.) using the `sum-buddy` library
- Optional image downsampling and resizing using Pillow
- Comprehensive logging of downloads and errors
- CSV validation and data integrity checks
- Support for organizing downloads into subdirectories

## Architecture and Code Organization

### Core Modules
- `__main__.py` - Main entry point with CLI argument parsing and workflow orchestration
- `download.py` - Core image downloading functionality with retry logic
- `buddy_check.py` - Checksum validation and download verification using BuddyCheck class
- `utils.py` - Helper functions for CSV processing, logging, and image downsampling
- `exceptions.py` - Custom exception classes

### Key Classes and Functions
- `BuddyCheck` - Main class for validating downloads against expected checksums
- `download_images()` - Core function for downloading images with progress tracking
- `process_csv()` - Validates and processes input CSV files
- `downsample_and_save_image()` - Handles image resizing operations

## Development Guidelines

### Code Style and Standards
- Follow PEP 8 style guidelines
- Use descriptive function and variable names
- Include comprehensive docstrings for all functions and classes
- Use type hints where appropriate
- Handle exceptions gracefully with meaningful error messages

### Testing Patterns
- Use `unittest` framework for test cases
- Create temporary files for testing file operations
- Test both success and failure scenarios
- Mock external dependencies (HTTP requests) when appropriate
- Include tests for edge cases (empty files, network errors, etc.)

### Dependencies Management
- Core dependencies: `requests`, `pandas`, `pillow`, `sum-buddy`, `argparse`
- Development dependencies: `pytest`, `ruff`, `pre-commit`
- Use `pyproject.toml` for project configuration
- Pin dependency versions for reproducible builds

## CLI Interface Patterns

### Required Arguments
- `-i, --input-file`: Path to CSV file with URLs
- `-o, --output-dir`: Directory for downloaded images

### Optional Arguments
- `-s, --subdir-col`: Column name for subdirectory organization
- `-n, --img-name-col`: Column for image filenames (default: "filename")
- `-u, --url-col`: Column with URLs (default: "file_url")
- `-w, --wait-time`: Retry delay in seconds (default: 3)
- `-r, --max-retries`: Maximum retry attempts for a single image (default: 5)
- `-x, --starting-idx`: Index of DataFrame from CSV at which to start download (default: 0)
- `-l, --side-length`: Pixels per side for square resized images
- `-a, --checksum-algorithm`: Hash algorithm for checksums (default: "md5")
- `-v, --verifier-col`: Column with expected checksums for validation

### CSV Format Expectations
- Required columns: filename and URL columns (customizable names)
- Optional columns: subdirectory organization, checksum verification
- Case-insensitive column matching (all converted to lowercase)
- Handle missing values gracefully

## Error Handling and Validation

### Input Validation
- Verify CSV file extension
- Check for required columns in CSV
- Validate filename uniqueness
- Prevent overwriting existing output directories
- Handle missing filenames with user prompts

### Download Reliability
- Implement retry logic with exponential backoff
- Log all HTTP response codes as strings (not integers)
- Create separate log files for successful downloads and errors
- Generate JSONL format logs for structured data

### Checksum Verification
- Support multiple hash algorithms via `hashlib`
- Compare downloaded file checksums with expected values
- Report mismatches and missing files in separate CSV
- Use `sum-buddy` library for checksum generation

## File Organization

### Output Structure
- Main directory: specified by `--output-dir`
- Optional subdirectories: based on `--subdir-col` values
- Downsampled images: `<output-dir>_downsized` directory
- Log files: same directory as input CSV with descriptive suffixes

### Logging Files
- `<csv_name>_log.jsonl` - Successful downloads
- `<csv_name>_error_log.jsonl` - Failed downloads
- `<csv_name>_checksums.csv` - Generated checksums
- `<csv_name>_missing.csv` - Verification mismatches

## Performance Considerations

### Image Processing
- Use Pillow for image operations
- Implement memory-efficient downsampling
- Handle various image formats gracefully
- Process images sequentially to avoid memory issues

### Progress Tracking
- Use `tqdm` for progress bars
- Report download statistics
- Provide clear status messages for user feedback

## Testing and CI/CD

### Test Structure
- Unit tests for individual functions
- Integration tests for full workflows
- Use temporary files for file I/O testing
- Test error conditions and edge cases

### GitHub Actions
- Run tests on Python 3.10, 3.11, 3.12, 3.13
- Use Ruff for linting
- Test on Ubuntu latest
- Separate workflows for PRs and pushes

### Pre-commit Hooks
- Ruff linting automatically applied
- Use `.pre-commit-config.yaml` for configuration
- Keep hooks updated with `pre-commit autoupdate`

## Common Patterns and Conventions

### DataFrame Operations
- Convert column names to lowercase for case-insensitive matching
- Use `pd.read_csv()` with `low_memory=False` for large files
- Filter DataFrames using `.loc[]` for better performance
- Handle NaN values explicitly

### Logging and Error Reporting
- Use structured logging with JSON (specifically JSONL) format
- Include context information (image name, URL, index)
- Provide user-friendly error messages
- Log response codes as strings to avoid serialization issues

### Configuration Management
- Use argument groups for required vs optional parameters
- Provide sensible defaults for optional parameters
- Validate user inputs before processing
- Support flexible column naming

## Examples and Usage

### Basic Usage
```bash
cautious-robot --input-file examples/HCGSD_testNA.csv --output-dir examples/test_images
```

### With Subdirectories and Checksums
```bash
cautious-robot -i data.csv -o images -s species -a sha256 -v expected_sha256
```

### With Downsampling
```bash
cautious-robot -i data.csv -o images -l 512
```

When implementing features or fixing bugs, consider the tool's primary use case in scientific image processing workflows, where data integrity and reliable downloads are critical.