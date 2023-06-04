import os.path
import time
from threading import Thread
from functools import partial
import collections
import argparse
import datetime
import logging
import re
import pytz

from multiparser.core.requests_handler import RequestsHandler
from multiparser.core.mthread_driver import MultiThreadDriver

from multiparser.core.request import Request
from multiparser.custom_exceptions.cex import MaxMemoryLimit
from multiparser.parsers.binance import BinanceHistoryGetter
from multiparser.drivers.binance import BinanceDriver
from multiparser.core.speedometer import Speedometer


class App:
    def __init__(self, driver_constructor: callable):
        self.rh = RequestsHandler(
            speedometer=Speedometer(max_speed=10 / 1),
            max_memory=20
        )
        self.mtp = MultiThreadDriver(
            self.rh,
            n_workers=2,
            driver_constructor=driver_constructor
        )

        self.worker = None

    def run(self):
        self.mtp.start()
        self.rh.start()

    def start(self):
        self.worker = Thread(target=self.run)
        self.worker.start()

    def stop(self, wait=True):
        while self.rh.num_requests_undone > 0 and wait:
            time.sleep(1)
        self.mtp.stop()
        self.rh.stop()
        self.join()

    def add_request(self, req: 'Request'):
        self.rh.add_request(req)

    @property
    def requests_done_data(self):
        return self.rh.requests_done_data

    def join(self):
        self.worker.join()


def save_request_data(request_data: 'RequestData', out_dir: str):
    if out_dir is None:
        return
    os.makedirs(out_dir, exist_ok=True)
    data = request_data.data[0]
    start_time = data['request'].kwargs['startTime']
    end_time = data['request'].kwargs['endTime']
    start_time = datetime.datetime.fromtimestamp(start_time / 1000).astimezone(pytz.utc).strftime('%Y%m%d%H%M')
    end_time = datetime.datetime.fromtimestamp(end_time / 1000).astimezone(pytz.utc).strftime('%Y%m%d%H%M')
    out_filename = os.path.join(out_dir, f'{start_time}_{end_time}.csv')
    df = data['data']
    if df is not None:
        df.to_csv(out_filename, index=False)
    return


def start_parsing(a: App, requests_to_make: collections.deque, args: 'Namespace') -> list:
    done_requests = list()
    a.add_request(requests_to_make.popleft())

    while a.rh.num_requests_undone or len(requests_to_make):
        while len(requests_to_make):
            req = requests_to_make.popleft()
            try:
                a.add_request(req)
            except MaxMemoryLimit:
                requests_to_make.appendleft(req)
                break
        while not a.requests_done_data.empty():
            done_req = a.requests_done_data.get()
            # req is: done_req = {part_id: {'data': return data, 'request': req}
            save_request_data(done_req, args.out_dir)
        # print(f'Completed {a.rh.num_containers_done} out of {a.rh.num_requests_to_do}')
        time.sleep(0.05)
    else:
        while not a.requests_done_data.empty():
            done_req = a.requests_done_data.get()
            # print(f'Completed {a.rh.num_containers_done} out of {a.rh.num_requests_to_do}')
            save_request_data(done_req, args.out_dir)
    return done_requests


def convert_time(args: 'Namespace') -> tuple:
    if args.time_end is None:
        time_end = datetime.datetime.now()
    else:
        time_end = datetime.datetime.strptime(args.time_end, "%Y-%m-%d")

    if args.time_start is None:
        time_start = time_end - datetime.timedelta(minutes=args.limit)
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


def main(args: 'Namespace'):
    from binance.spot import Spot

    api_key = open(args.api_key).readline().strip()
    api_secret = open(args.api_secret).readline().strip()

    client = Spot(api_key, api_secret)
    a = App(partial(BinanceDriver, client))

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
    d = start_parsing(a, dq, args)
    time.sleep(2)
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
    parser.add_argument('--out_dir', type=str, help='out dir path')
    return parser.parse_args()


if __name__ == '__main__':
    # https://github.com/binance/binance-connector-python
    logging.getLogger().setLevel(logging.DEBUG)
    args = parse_args()
    args.api_key = "C:/projects/newl/trading/code/binance/secrets/init.txt"
    args.api_secret = "C:/projects/newl/trading/code/binance/secrets/init_secret.txt"
    args.out_dir = "C:/projects/newl/trading/data/btcusdt/"
    args.time_start = "2020-01-01"
    main(args)
