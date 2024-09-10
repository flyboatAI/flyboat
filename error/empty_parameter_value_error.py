class EmptyParameterValueError(Exception):
    def __init__(self, empty_value_list=None):
        super().__init__(self)
        if empty_value_list:
            error_message = f"参数 {'、'.join(empty_value_list)} 不允许为空, 请检查"
            self.error_message = error_message
        else:
            self.error_message = "存在必填参数值为空, 请检查"

    def __str__(self):
        return self.error_message
