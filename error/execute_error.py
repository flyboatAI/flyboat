class ExecuteError(Exception):
    def __init__(self, func_name, error_message):
        super().__init__(self)
        self.func_name = func_name
        self.error_message = error_message

    def __str__(self):
        return self.error_message
