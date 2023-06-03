from multiparser.core.single_driver import SingleDriver


class BinanceDriver(SingleDriver):
    def __init__(self, client: 'Client', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = client
