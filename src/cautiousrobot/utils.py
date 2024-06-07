# Helper functions for download

import json

def log_response(log_data, index, image, url, response_code):
    # log status
    log_entry = {}
    log_entry["Image"] = image
    log_entry["file_url"] = url
    log_entry["Response_status"] = str(response_code) #int64 has problems sometimes
    log_data[index] = log_entry

    return log_data


def update_log(log, index, filepath):
    # save logs
    with open(filepath, "a") as log_file:
        json.dump(log[index], log_file, indent = 4)
        log_file.write("\n")
