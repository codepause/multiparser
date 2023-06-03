class MaxMemoryLimit(Exception):
    def __init__(self, message: str):
        super(MaxMemoryLimit, self).__init__(message)
