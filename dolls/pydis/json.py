from rejson import Client


class Json(Client):

    def __init__(self, connection_pool):
        super().__init__(connection_pool = connection_pool)
