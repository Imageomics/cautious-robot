# cautious-robot

I am a simple downloader that downloads images from URLs in a CSV and names them by the given column. After download, [`sum-buddy`](https://github.com/Imageomics/sum-buddy) helps me gather and record checksums for all downloaded images.

## Requirements
Python 3.7+

## Installation
```bash
pip install git+https://github.com/Imageomics/cautious-robot
```

## How it Works

Cautious-robot will check the provided CSV for `IMG_NAME`, `URL`, and `SUBFOLDERS` (if provided), then download all images that have a value in the `IMG_NAME` column. Note that choice of image filename should be unique, cautious-robot will refuse the request if the filename column selected is not a unique identifier for the dataset. Images that have a filename but no `URL` are recorded in the error log. If desired, a secondary output directory will be created with square copies of the images downsampled to the desired size (e.g., 256 x 256). Parameters such as time to wait between retries, the maximum number of times to try retrying an image, and which index of the CSV to start with can all also be passed. Cautious-robot will retry image downloads when the following responses are returned: `429, 500, 502, 503, 504`.

After downloading the images, cautious-robot calls [`sum-buddy`](https://github.com/Imageomics/sum-buddy) to calculate and record checksums of the `OUTPUT` folder contents.

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
cautious-robot --input-file examples/HCGSD_testNA.csv --output-dir examples/test_images
```
 > Output:
 > ```console
 > 100%|██████████████████████████████████████████████████████████████████| 8/8 [00:01<00:00,  4.18it/s]
 > Images downloaded from examples/HCGSD_testNA.csv to examples/test_images.
 > Download logs are in examples/HCGSD_testNA_log.jsonl and examples/HCGSD_testNA_error_log.jsonl.
 > Calculating md5 checksums on examples/test_images: 100%|███████████████████████████████████████████| 16/16 [00:00<00:00, 3133.00it/s]
 > md5 checksums for examples/test_images written to examples/HCGSD_testNA_checksums.csv
 > ```

- **Download Images to Subfolders Based on Column Value:**
```
cautious-robot -i examples/HCGSD_testNA.csv -o examples/test_images --subdir-col Species
```
 > Output:
 > ```console
 > 100%|██████████████████████████████████████████████████████████████████████████████████████████████████| 8/8 [00:02<00:00,  3.47it/s]
 > Images downloaded from examples/HCGSD_testNA.csv to examples/test_images.
 > Download logs are in examples/HCGSD_testNA_log.jsonl and examples/HCGSD_testNA_error_log.jsonl.
 > Calculating md5 checksums on examples/test_images: 100%|█████████████████████████████████████████████| 8/8 [00:00<00:00, 3106.60it/s]
 > md5 checksums for examples/test_images written to examples/HCGSD_testNA_checksums.csv
 > ```
```
ls examples/test_images
```
 > Output:
 > ```console
 > erato	melpomene
 > ```

```
head -n 9 examples/HCGSD_testNA_checksums.csv
```
 > Output:
 > ```console
 > filepath,filename,md5
 > examples/test_images/erato/10429021_V_lowres,10429021_V_lowres,c6aeb9d2f6db412ff5be0eb0b5435b83
 > examples/test_images/erato/10428595_D_lowres,10428595_D_lowres,55882a0f3fdf8a68579c07254395653b
 > examples/test_images/erato/10428972_V_lowres,10428972_V_lowres,0047e7454ce444f67fee1c90cc3ba9cb
 > examples/test_images/erato/10428803_D_lowres,10428803_D_lowres,d8bfb73f2d3556390de04aa98822b815
 > examples/test_images/melpomene/10428169_V_lowres,10428169_V_lowres,042c9dc294d589ce3f140f14ddab0166
 > examples/test_images/melpomene/10428321_D_lowres,10428321_D_lowres,fbeeed30274e424831b06360b587ceb3
 > examples/test_images/melpomene/10428140_V_lowres,10428140_V_lowres,c11538f2de5a5e2d6013fc800848d43a
 > examples/test_images/melpomene/10428250_V_lowres,10428250_V_lowres,14ac99b1a9913a9d420f21b94d6136d6
 > ```

[1] The test images are from the [Cuthill Gold Standard Dataset](https://huggingface.co/datasets/imageomics/Curated_GoldStandard_Hoyal_Cuthill), which was processed from Cuthill, et. al. (original dataset available at [doi:10.5061/dryad.2hp1978](https://doi.org/10.5061/dryad.2hp1978)).