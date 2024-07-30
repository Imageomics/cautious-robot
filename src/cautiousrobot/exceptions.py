class EmptyDataFrameError(Exception):
    def __init__(self, df_name):
        message = f"Input DataFrame {df_name} is empty."
        super().__init__(message)
