class EmptyDataFrameError(Exception):
    def __init__(self, df_name):
        message = f"Input DataFrame {df_name} is empty."
        super().__init__(message)

class MissingColumnsError(Exception):
    def __init__(self, csv_path, column_names, expected_columns):
        message = f"The CSV at {csv_path} is missing column(s): {column_names}, defined as {expected_columns}."
        super().__init__(message)

class ImageResizeError(Exception):
    def __init__(self, image_name, reason):
            message = f"Failed to resize image '{image_name}': {reason}"
            super().__init__(message)