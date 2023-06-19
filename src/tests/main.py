from functools import partial
import collections
import argparse
import datetime
import logging
import re

from multiparser.core.request import Request

from multiparser.parsers.binance import BinanceHistoryGetter
from multiparser.drivers.binance import BinanceDriver

from app import App


def convert_time(args: 'Namespace') -> tuple:
    if args.time_end is None:
        time_end = datetime.datetime.now()
    else:
        time_end = datetime.datetime.strptime(args.time_end, "%Y-%m-%d")

    if args.time_start is None:
        time_start = time_end - datetime.timedelta(minutes=args.limit * 10)
    else:
        time_start = datetime.datetime.strptime(args.time_start, "%Y-%m-%d")

    return time_start, time_end


def get_timedelta(granularity: str) -> datetime.timedelta:
    value = re.findall(r'\d+', granularity)[0]
    return datetime.timedelta(minutes=int(value))


def split_time(time_start: datetime.datetime, time_end: datetime.datetime, granularity: str, step: int) -> list:
    timedelta = get_timedelta(granularity)
    timedelta = timedelta * step
    splits = list()
    temp = time_end
    while temp > time_start:
        splits.append((temp - timedelta, temp))
        temp -= timedelta
    return splits


class PlaceholderClient:
    # class for demo only, matches only .klines fnc in binance driver
    def __init__(self, *_, **__):
        pass

    def klines(self, *_, **__) -> None:
        return None


def get_driver_builder(args: 'Namespace', typ: str = 'Spot'):
    if typ == 'Spot':
        from binance.spot import Spot
        api_key = open(args.api_key).readline().strip()
        api_secret = open(args.api_secret).readline().strip()

        client = Spot(api_key, api_secret)
        return partial(BinanceDriver, client)
    else:
        client = PlaceholderClient()
        return partial(BinanceDriver, client)


def main(args: 'Namespace'):
    builder = get_driver_builder(args, 'test')
    a = App(builder)

    getter = BinanceHistoryGetter()

    dq = collections.deque()

    time_start, time_end = convert_time(args)
    times = split_time(time_start, time_end, args.granularity, args.limit)

    for (local_time_start, local_time_end) in times:
        local_time_start = round(local_time_start.timestamp() * 1000)
        local_time_end = round(local_time_end.timestamp() * 1000)
        dq.append(Request(getter, args=(args.ticker, args.granularity),
                          kwargs={'limit': args.limit, 'startTime': local_time_start, 'endTime': local_time_end}))

    a.start()
    a.start_parsing(dq, args.out_dir, verbose=True)
    a.stop()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--api_key', type=str, help='path to api key file', default='')
    parser.add_argument('--api_secret', type=str, help='path to api secret file', default='')
    parser.add_argument('--ticker', type=str, help='ticker name', default="BTCUSDT")
    parser.add_argument('--granularity', type=str, help='granularity', default='1m')
    parser.add_argument('--limit', type=int, help='amount of inclusions. 1k max for 1m tick', default=1000)
    parser.add_argument('--time_start', type=str, help="time start. YYYY-MM-DD", default=None)
    parser.add_argument('--time_end', type=str, help="time end. YYYY-MM-DD", default=None)
    parser.add_argument('--out_dir', type=str, help='out dir path', default=None)
    return parser.parse_args()


if __name__ == '__main__':
    # https://github.com/binance/binance-connector-python
    logging.getLogger().setLevel(logging.DEBUG)
    args = parse_args()
    main(args)
