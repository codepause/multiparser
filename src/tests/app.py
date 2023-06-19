import os.path
import time
from threading import Thread
import collections
import datetime
import pytz

from multiparser.core.requests_handler import RequestsHandler
from multiparser.core.mthread_driver import MultiThreadDriver
from multiparser.core.request import Request
from multiparser.custom_exceptions.cex import MaxMemoryLimit
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

    def start_parsing(self, requests_to_make: collections.deque, out_dir: str = None) -> list:
        done_requests = list()
        self.add_request(requests_to_make.popleft())

        while self.rh.num_requests_undone or len(requests_to_make):
            while len(requests_to_make):
                req = requests_to_make.popleft()
                try:
                    self.add_request(req)
                except MaxMemoryLimit:
                    requests_to_make.appendleft(req)
                    break
            while not self.requests_done_data.empty():
                done_req = self.requests_done_data.get()
                # req is: done_req = {part_id: {'data': return data, 'request': req}
                self.save_request_data(done_req, out_dir)
            # print(f'Completed {self.rh.num_containers_done} out of {self.rh.num_requests_to_do}')
            time.sleep(0.05)
        else:
            while not self.requests_done_data.empty():
                done_req = self.requests_done_data.get()
                # print(f'Completed {self.rh.num_containers_done} out of {self.rh.num_requests_to_do}')
                self.save_request_data(done_req, out_dir)
        return done_requests
