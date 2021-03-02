from rejson import Client


class Json(Client):

    def __init__(self, encoder=None, decoder=None, *args, **kwargs):
        super().__init__(encoder, decoder, *args, **kwargs)
