# cautious-robot

I am a simple downloader that downloads images from URLs in a CSV and names them by the given column. After download, [`sum-buddy`](https://github.com/Imageomics/sum-buddy) helps me gather and record checksums for all downloaded images.

## Requirements
Python 3.7+

## Installation
```bash
pip install git+https://github.com/Imageomics/cautious-robot
```

## How it Works

Cautious-robot will check the provided CSV for `IMG_NAME`, `URL`, and `SUBFOLDERS` (if provided), then download all images that have a value in the `IMG_NAME` column. Note that choice of image filename should be unique , especially if working in a flat directory, as the images will otherwise be overwritten. Images that have a filename but no `URL` are recorded in the error log. After downloading the images, cautious-robot calls [`sum-buddy`](https://github.com/Imageomics/sum-buddy) to calculate and record checksums of the `OUTPUT` folder contents.

### Command Line Usage
```
usage: cautious-robot [-h] -i [INPUT_FILE] -o [OUTPUT_DIR] [-s [SUBDIR_COL]] [-n [IMG_NAME_COL]] [-u [URL_COL]] [-w WAIT_TIME]
                      [-r MAX_RETRIES] [-l SIDE_LENGTH] [-x STARTING_IDX] [-a CHECKSUM_ALGORITHM]

options:
  -h, --help            show this help message and exit

required arguments:
  -i [INPUT_FILE], --input-file [INPUT_FILE]
                        path to CSV file with urls.
  -o [OUTPUT_DIR], --output-dir [OUTPUT_DIR]
                        main directory to download images into.

optional arguments:
  -s [SUBDIR_COL], --subdir-col [SUBDIR_COL]
                        name of column to use for subfolders in image directory (defaults to flat directory if left blank)
  -n [IMG_NAME_COL], --img-name-col [IMG_NAME_COL]
                        column to use for image filename (default: filename)
  -u [URL_COL], --url-col [URL_COL]
                        column with URLs to download (default: file_url)
  -w WAIT_TIME, --wait-time WAIT_TIME
                        time to wait between tries (default: 3)
  -r MAX_RETRIES, --max-retries MAX_RETRIES
                        max times to retry download on a single image (default: 5)
  -l SIDE_LENGTH, --side-length SIDE_LENGTH
                        number of pixels per side for downsampled images (default: no downsized images created)
  -x STARTING_IDX, --starting-idx STARTING_IDX
                        index of CSV at which to start download (default: 0)
  -a CHECKSUM_ALGORITHM, --checksum-algorithm CHECKSUM_ALGORITHM
                        checksum algorithm to use on images (default: md5, available: sha3_512, md5-sha1, shake_256, sha3_384,
                        sha3_224, sha384, sm3, sha224, md5, sha512_256, sha512, blake2s, ripemd160, sha256, sha3_256, sha1,
                        sha512_224, shake_128, blake2b)
```
Note: Alternate checksum options are [pending](https://github.com/Imageomics/sum-buddy/pull/8).

#### CLI Examples

Sample CSVs [1] are provided in the `examples/` directory to test the CLI.

- **Defaults:**
```
cautious-robot --input-file "examples/HCGSD_testNA.csv" --output-dir "examples/test_images"
```
 > Output:
 > ```console
 > 100%|██████████████████████████████████████████████████████████████████| 8/8 [00:01<00:00,  4.18it/s]
 > Images downloaded from examples/HCGSD_testNA.csv to examples/test_images.
 > Download logs are in examples/HCGSD_testNA_log.jsonl and examples/HCGSD_testNA_error_log.jsonl.
 > MD5ing: 100%|████████████████████████████████████████████████████████████████████████████████████████| 8/8 [00:00<00:00, 5280.84it/s]
 > Checksums written to examples/HCGSD_testNA_checksums.csv
 > ```

- **Download Images to Subfolders:**
```
cautious-robot -i "examples/HCGSD_testNA.csv" -o "examples/test_images" --subdir-col "Species"
```
 > Output:
 > ```console
 > 100%|██████████████████████████████████████████████████████████████████████████████████████████████████| 8/8 [00:02<00:00,  3.47it/s]
 > Images downloaded from examples/HCGSD_testNA.csv to examples/test_images.
 > Download logs are in examples/HCGSD_testNA_log.jsonl and examples/HCGSD_testNA_error_log.jsonl.
 > MD5ing: 100%|████████████████████████████████████████████████████████████████████████████████████████| 8/8 [00:00<00:00, 1962.71it/s]
 > MD5ing: 100%|████████████████████████████████████████████████████████████████████████████████████████| 4/4 [00:00<00:00, 4329.60it/s]
 > MD5ing: 100%|████████████████████████████████████████████████████████████████████████████████████████| 4/4 [00:00<00:00, 2389.24it/s]
 > Checksums written to examples/HCGSD_testNA_checksums.csv
 > ```
```
ls examples/test_images
```
 > Output:
 > ```console
 > erato	melpomene
 > ```

[1] The test images are from the [Cuthill Gold Standard Dataset](https://huggingface.co/datasets/imageomics/Curated_GoldStandard_Hoyal_Cuthill), which was processed from Cuthill, et. al. (original dataset available at [doi:10.5061/dryad.2hp1978](https://doi.org/10.5061/dryad.2hp1978)).