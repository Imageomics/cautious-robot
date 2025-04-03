# cautious-robot

<img align="right" src="cautious-robot_logo.png" alt="cautious-robot logo, an image of a robot generated with Canva Magic Media" width="384"/>

I am a simple downloader that downloads images from URLs in a CSV and names them by the given column (after ensuring all its values are unique). I can organize your images into subfolders based on any column in your CSV and will warn you if the parent image folder already exists before overwriting it. If you need square images for modeling, I'll create a second directory (organized in the same format) with downsized copies of your images. Patience is a virtue, so I will wait a designated time before re-requesting an image after receiving an error on my retry list; if all retries are expended or I receive another error, I log that for your review and move on. I also keep a log of all successful responses. After download, [`sum-buddy`](https://github.com/Imageomics/sum-buddy) helps me gather and record checksums for all downloaded images. If the source CSV has a checksum column, I can then do a buddy-check to verify all expected images are downloaded intact. At a minimum, I check the number of expected images matches the number sum-buddy counts.

  
<p align="right">
  <sub><sup>The Cautious Robot Logo was designed using <a href="https://www.canva.com/ai-image-generator/">Canva Magic Media</a>.</sup></sub>
  </p>


## Requirements
Python 3.10+

## Installation
```bash
pip install cautious-robot
```

## How it Works

Cautious-robot will check the provided CSV for `IMG_NAME`, `URL`, and `SUBFOLDERS` (if provided), then download all images that have a value in the `IMG_NAME` column. Note that choice of image filename should be unique; cautious-robot will refuse the request if the filename column selected is not unique within the dataset. It will also check if the provided `OUTPUT` folder already exists, asking the user before proceeding. Images that have a filename but no `URL` are recorded in the error log; the user is prompted whether to ignore or address the missing URLs prior to downloading. Logs are saved in the same directory as the source CSV (logging is done by adding to an existing JSON, so it will not overwrite existing logs with the same name in case of a restarted download). Please note that if the streamed response is interrupted before the image is downloaded in its entirety this error may not be recorded in the error log, but the verifier would register them as missing.

If desired, a secondary output directory (`OUTPUT_downsized`) will be created with square copies of the images downsized to the specified size (e.g., 256 x 256). The folder structure of this secondary output directory will match that of the un-processed images. Parameters such as time to wait between retries on a failed download, the maximum number of times to retry downloading an image, and which index of the CSV to start with can all also be passed. Cautious-robot will retry image downloads when receiving one of the following [HTTP response status codes](https://en.wikipedia.org/wiki/List_of_HTTP_status_codes): `429, 500, 502, 503, 504`.

After downloading the images, cautious-robot calls [`sum-buddy`](https://github.com/Imageomics/sum-buddy) to calculate and record checksums of the `OUTPUT` folder contents. It prints the number of images contained in the `OUTPUT` folder along with the expected number (based on a count of the unique, non-null filenames in the source file). If provided a column with checksums in the source file, it will then further verify that all expected images are downloaded through an inner merge on the checksum and filename columns of the source file with the checksum CSV (thus avoiding confusion in case of duplicate images).

### Command Line Usage
```
usage: cautious-robot [-h] -i [INPUT_FILE] -o [OUTPUT_DIR] [-s [SUBDIR_COL]] [-n [IMG_NAME_COL]] [-u [URL_COL]] [-w WAIT_TIME]
                      [-r MAX_RETRIES] [-l SIDE_LENGTH] [-x STARTING_IDX] [-a CHECKSUM_ALGORITHM] [-v [VERIFIER_COL]]

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
                        seconds to wait between retries for an image (default: 3)
  -r MAX_RETRIES, --max-retries MAX_RETRIES
                        max times to retry download on a single image (default: 5)
  -l SIDE_LENGTH, --side-length SIDE_LENGTH
                        number of pixels per side for resized square images (default: no resized images created)
  -x STARTING_IDX, --starting-idx STARTING_IDX
                        index of CSV at which to start download (default: 0)
  -a CHECKSUM_ALGORITHM, --checksum-algorithm CHECKSUM_ALGORITHM
                        checksum algorithm to use on images (default: md5, available: sha256, sha384, md5-sha1, blake2b, sha512,
                        sha1, sm3, sha3_256, sha512_256, sha224, sha3_224, ripemd160, sha3_384, shake_128, blake2s, md5, sha3_512,
                        sha512_224, shake_256)
  -v [VERIFIER_COL], --verifier-col [VERIFIER_COL]
                        name of column in source CSV with checksums (same hash as -a) to verify download
```

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
 > 8 images were downloaded to examples/test_images of the 8 expected.
 > ```
```
head -n 9 examples/HCGSD_testNA_checksums.csv
```
 > Output:
 > ```console
 > filepath,filename,md5
 > examples/test_images/10429021_V_lowres.jpg,10429021_V_lowres.jpg,c6aeb9d2f6db412ff5be0eb0b5435b83
 > examples/test_images/10428595_D_lowres.jpg,10428595_D_lowres.jpg,55882a0f3fdf8a68579c07254395653b
 > examples/test_images/10428972_V_lowres.jpg,10428972_V_lowres.jpg,0047e7454ce444f67fee1c90cc3ba9cb
 > examples/test_images/10428803_D_lowres.jpg,10428803_D_lowres.jpg,d8bfb73f2d3556390de04aa98822b815
 > examples/test_images/10428169_V_lowres.jpg,10428169_V_lowres.jpg,042c9dc294d589ce3f140f14ddab0166
 > examples/test_images/10428321_D_lowres.jpg,10428321_D_lowres.jpg,fbeeed30274e424831b06360b587ceb3
 > examples/test_images/10428140_V_lowres.jpg,10428140_V_lowres.jpg,c11538f2de5a5e2d6013fc800848d43a
 > examples/test_images/10428250_V_lowres.jpg,10428250_V_lowres.jpg,14ac99b1a9913a9d420f21b94d6136d6
 > ```

- **Download Images to Subfolders Based on Column Value:**
```
cautious-robot -i examples/HCGSD_testNA.csv -o examples/test_images_subdirs --subdir-col Species
```
 > Output:
 > ```console
 > 100%|██████████████████████████████████████████████████████████████████████████████████████████████████| 8/8 [00:02<00:00,  3.47it/s]
 > Images downloaded from examples/HCGSD_testNA.csv to examples/test_images_subdirs.
 > Download logs are in examples/HCGSD_testNA_log.jsonl and examples/HCGSD_testNA_error_log.jsonl.
 > Calculating md5 checksums on examples/test_images_subdirs: 100%|█████████████████████████████████████████████| 8/8 [00:00<00:00, 3106.60it/s]
 > md5 checksums for examples/test_images_subdirs written to examples/HCGSD_testNA_checksums.csv
 > 8 images were downloaded to examples/test_images_subdirs of the 8 expected.
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
 > examples/test_images_subdirs/erato/10429021_V_lowres.jpg,10429021_V_lowres.jpg,c6aeb9d2f6db412ff5be0eb0b5435b83
 > examples/test_images_subdirs/erato/10428595_D_lowres.jpg,10428595_D_lowres.jpg,55882a0f3fdf8a68579c07254395653b
 > examples/test_images_subdirs/erato/10428972_V_lowres.jpg,10428972_V_lowres.jpg,0047e7454ce444f67fee1c90cc3ba9cb
 > examples/test_images_subdirs/erato/10428803_D_lowres.jpg,10428803_D_lowres.jpg,d8bfb73f2d3556390de04aa98822b815
 > examples/test_images_subdirs/melpomene/10428169_V_lowres.jpg,10428169_V_lowres.jpg,042c9dc294d589ce3f140f14ddab0166
 > examples/test_images_subdirs/melpomene/10428321_D_lowres.jpg,10428321_D_lowres.jpg,fbeeed30274e424831b06360b587ceb3
 > examples/test_images_subdirs/melpomene/10428140_V_lowres.jpg,10428140_V_lowres.jpg,c11538f2de5a5e2d6013fc800848d43a
 > examples/test_images_subdirs/melpomene/10428250_V_lowres.jpg,10428250_V_lowres.jpg,14ac99b1a9913a9d420f21b94d6136d6
 > ```

- **Image Checksum Mismatch:** one value is intentionally altered in the source CSV
```
cautious-robot -i examples/HCGSD_test_MD5_mismatch.csv -o examples/test_images_md5_mismatch --subdir-col Species -v "md5"
```
 > Output:
 > ```console
 > 100%|██████████████████████████████████████████████████████████████████████████████████████████████████| 8/8 [00:01<00:00,  4.23it/s]
 > Images downloaded from examples/HCGSD_test_MD5_mismatch.csv to examples/test_images_md5_mismatch.
 > Download logs are in examples/HCGSD_test_MD5_mismatch_log.jsonl and examples/HCGSD_test_MD5_mismatch_error_log.jsonl.
 > Calculating md5 checksums on examples/test_images_md5_mismatch: 100%|████████████████████████████████| 8/8 [00:00<00:00, 4159.98it/s]
 > md5 checksums for examples/test_images_md5_mismatch written to examples/HCGSD_test_MD5_mismatch_checksums.csv
 > 8 images were downloaded to examples/test_images_md5_mismatch of the 8 expected.
 > Image mismatch: 1 image(s) not aligned, see examples/HCGSD_test_MD5_mismatch_missing.csv for missing image info and check logs.
 > ```
```
# Check on that mis-aligned image
head -n 2 examples/HCGSD_test_MD5_mismatch_missing.csv
```
 > Output:
 > ```console
 > nhm_specimen,species,subspecies,sex,file_url,filename,md5
 > 10428972,erato,petiverana,male,https://github.com/Imageomics/dashboard-prototype/raw/main/test_data/images/ventral_images/10428972_V_lowres.png,10428972_V_lowres.jpg,mismatch
 > ```

## Development
To develop the package further:

1. Clone the repository and create a branch.
2. Install with dev dependencies:
```bash
pip install -e ".[dev]"
```
3. Install pre-commit hook:
```bash
pre-commit install
pre-commit autoupdate # optionally update
```
4. Run tests:
```bash
pytest
```

[1] The test images are from the [Cuthill Gold Standard Dataset](https://huggingface.co/datasets/imageomics/Curated_GoldStandard_Hoyal_Cuthill), which was processed from Cuthill, et. al. (original dataset available at [doi:10.5061/dryad.2hp1978](https://doi.org/10.5061/dryad.2hp1978)).
