import time
from threading import Thread
from functools import partial
import collections
import argparse

import logging

logging.getLogger().setLevel(logging.DEBUG)

from multiparser.core.requests_handler import RequestsHandler
from multiparser.core.mthread_driver import MultiThreadDriver

from multiparser.core.request import Request
from multiparser.custom_exceptions.cex import MaxMemoryLimit
from multiparser.parsers.binance import BinanceHistoryGetter
from multiparser.drivers.binance import BinanceDriver


class App:
    def __init__(self, driver_constructor: callable):
        self.rh = RequestsHandler(
            max_memory=10
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


def start_parsing(a: App, requests_to_make: collections.deque):
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
            done_requests.append(done_req)
        print(f'Completed {a.rh.num_containers_done} out of {a.rh.num_requests_to_do}')
        time.sleep(0.5)
    else:
        while not a.requests_done_data.empty():
            done_req = a.requests_done_data.get()
            print(f'Completed {a.rh.num_containers_done} out of {a.rh.num_requests_to_do}')
            done_requests.append(done_req)
    return done_requests


def main(args: 'Namespace'):
    from binance.spot import Spot

    api_key = open(args.api_key).readline().strip()
    api_secret = open(args.api_secret).readline().strip()

    client = Spot(api_key, api_secret)
    a = App(partial(BinanceDriver, client))

    getter = BinanceHistoryGetter()

    dq = collections.deque()
    words = [1685795339999, 1685095339999, 1685095339999, 1685095339999]
    for word in words:
        dq.append(Request(getter, args=("BTCUSDT", "1m"), kwargs={'limit': 1000}))

    a.start()
    d = start_parsing(a, dq)
    time.sleep(2)
    a.stop()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--api_key', type=str, help='path to api key file', default='')
    parser.add_argument('--api_secret', type=str, help='path to api secret file', default='')
    return parser.parse_args()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    args = parse_args()
    args.api_key = "C:/projects/newl/trading/binance/secrets/init.txt"
    args.api_secret = "C:/projects/newl/trading/binance/secrets/init_secret.txt"
    main(args)
