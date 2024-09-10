class NoSuchOssTypeError(Exception):
    def __init__(self, error_message):
        super().__init__(self)
        self.error_message = error_message

    def __str__(self):
        return self.error_message
