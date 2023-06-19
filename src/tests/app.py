import os.path
import time
from threading import Thread
import collections
import datetime
import tqdm
import pytz

from multiparser.core.requests_handler import RequestsHandler
from multiparser.core.mthread_driver import MultiThreadDriver
from multiparser.core.request import Request
from multiparser.custom_exceptions.cex import MaxMemoryLimit
from multiparser.core.speedometer import Speedometer


def _envoke_update(slider: tqdm.tqdm, item: int) -> None:
    slider.n = item
    slider.refresh()


def _envoke_total(slider: tqdm.tqdm, item: int) -> None:
    slider.total = item
    slider.refresh()


class App:
    """
    Args:
        driver_constructor (callable): driver constructor to call in thread.
    Keyword Args:
        n_workers (int): N threads to create.
        rps (int): max requests per second speed.
        max_memory (int): num of simultaneous requests to store in memory.
    """

    def __init__(self, driver_constructor: callable, n_workers: int = 2, rps: int = 10, max_memory: int = 20):
        self.rh = RequestsHandler(
            speedometer=Speedometer(max_speed=rps),
            max_memory=max_memory
        )
        self.mtp = MultiThreadDriver(
            self.rh,
            n_workers=n_workers,
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

    @staticmethod
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

    def _gather_loop(self, slider: tqdm.tqdm, out_dir: str, meta: dict):
        while not self.requests_done_data.empty():
            done_req = self.requests_done_data.get()
            # req is: done_req = {part_id: {'data': return data, 'request': req}
            self.save_request_data(done_req, out_dir)
            meta['requests_get_num'] += 1
            _envoke_update(slider, meta['requests_get_num'])

    def _put_loop(self, requests_to_make: collections.deque, slider: tqdm.tqdm, meta: dict):
        while len(requests_to_make):
            req = requests_to_make.popleft()
            try:
                self.add_request(req)
                meta['requests_post_num'] += 1
                _envoke_total(slider, meta['requests_post_num'])
            except MaxMemoryLimit:
                requests_to_make.appendleft(req)
                break

    def start_parsing(self, requests_to_make: collections.deque, out_dir: str = None, verbose: bool = False) -> list:
        done_requests = list()

        meta = {'requests_post_num': 0, 'requests_get_num': 0, 'requests_total_num': len(requests_to_make)}
        with tqdm.tqdm(total=1, disable=not verbose, desc=f'total: {meta["requests_total_num"]}; post/get') as slider:
            while self.rh.num_requests_undone or len(requests_to_make):
                self._put_loop(requests_to_make, slider, meta)
                self._gather_loop(slider, out_dir, meta)
                time.sleep(0.05)
            else:
                self._gather_loop(slider, out_dir, meta)
        return done_requests
