from multiparser.core.single_driver import SingleDriverBase


class BinanceDriver(SingleDriverBase):
    def __init__(self, client: 'Client', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = client
