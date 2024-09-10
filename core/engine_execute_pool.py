class ExecutePool(object):
    __pool = None
    __manager_dict = None

    @classmethod
    def get_pool(cls):
        return cls.__pool

    @classmethod
    def set_pool(cls, pool):
        cls.__pool = pool

    @classmethod
    def get_manager_dict(cls):
        return cls.__manager_dict

    @classmethod
    def set_manager_dict(cls, manager_dict):
        cls.__manager_dict = manager_dict
