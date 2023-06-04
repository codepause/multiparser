import pandas as pd
from datetime import datetime, timezone
import pytz

from multiparser.core.parser import Parser
from multiparser.core.request import Request
from multiparser.drivers.binance import BinanceDriver


class BinanceHistoryGetter(Parser):
    def __init__(self):
        self.df_columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close',
                           'volume', 'quote_asset_volume', 'num_trades', 'taker_buy_base_asset_volume',
                           'taker_buy_quote_asset_volume', 'ignore', 'open_timestamp', 'close_timestamp']

    def __call__(self, single_driver: BinanceDriver, request: Request, *args, **kwargs):
        data = single_driver.client.klines(*args, **kwargs)
        if data:
            df = pd.DataFrame.from_records(data, columns=['open_timestamp', 'open', 'high', 'low', 'close', 'volume',
                                                          'close_timestamp', 'quote_asset_volume', 'num_trades',
                                                          'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
                                                          'ignore'])
            df['open_time'] = df.open_timestamp.apply(lambda ts: datetime.fromtimestamp(ts / 1000).astimezone(pytz.utc))
            df['close_time'] = df.close_timestamp.apply(lambda ts: datetime.fromtimestamp(ts / 1000).astimezone(pytz.utc))
            df = df[self.df_columns].sort_values('open_time', ascending=True)
        else:
            df = None
        return df
