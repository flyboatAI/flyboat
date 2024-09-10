class LicenseError(Exception):
    def __init__(self, error_message="授权证书校验失败, 请联系官方客服人员"):
        super().__init__(self)
        self.error_message = error_message

    def __str__(self):
        return self.error_message
