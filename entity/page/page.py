class Page:
    def __init__(self, data: list | None = None, count: int | None = 0):
        """
        分页返回对象实体
        :param data: 数据
        :param count: 总量
        """
        self.data = data if data else []
        self.count = count
