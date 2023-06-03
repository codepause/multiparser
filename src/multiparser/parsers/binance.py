from multiparser.core.parser import Parser
from multiparser.core.request import Request
from multiparser.drivers.binance import BinanceDriver


class BinanceHistoryGetter(Parser):
    def __call__(self, single_driver: BinanceDriver, request: Request, *args, **kwargs):
        data = single_driver.client.klines(*args, **kwargs)
        return data
