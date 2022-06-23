from rejson import Client, Path


class Json(Client):

    def __init__(self, connection_pool):
        super().__init__(connection_pool=connection_pool)

    def insert_append(self, key, value):
        result = self.jsonget(key) or {}
        # update
        v = self.__update(result, value)

        return self.jsonset(key, Path.rootPath(), v)

    def __update(self, result, value):

        for k, v in value.items():
            if k in result:
                if type(v) is list:
                    result[k].extend(v)
                elif type(v) is dict:
                    result[k] = self.__update(result[k], v)
                elif type(v) is str:
                    result[k] = result[k] + v
            else:
                result[k] = v

        return result

    insert_update = insert_append
