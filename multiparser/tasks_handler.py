import threading
from queue import Queue
from collections import OrderedDict

from custom_exceptions.cex import *
import time
from threading import Thread


class Request:
    def __init__(self, parser, *args, **kwargs):
        self.parent_idx = None
        self.parser = parser
        self.args = args
        self.kwargs = kwargs
        self.total_parts = kwargs.get('total_parts', 0)
        self.current_part = kwargs.get('current_part', 0)

    def is_last(self):
        return self.total_parts == self.current_part

    def __call__(self, single_parser):
        return self.parser(single_parser, self, *self.args, **self.kwargs)

    def __repr__(self):
        s = f'{self.__class__.__class__}[{self.args}, {self.kwargs}]'
        return s


class RequestData:
    def __init__(self):
        self.total_parts = -1
        self.parts_done = -1
        self.data = dict()

    def add_data(self, job_result):
        req = job_result['request']
        data = job_result['data']
        if req.is_last():
            self.total_parts = req.total_parts
        self.data[req.current_part] = {'data': data, 'request': req}
        self.parts_done += 1

    def is_completed(self):
        return self.total_parts >= self.parts_done

    def __repr__(self):
        s = f'{self.__class__.__name__}:[\n{self.data}\n]\n[{self.parts_done}/{self.total_parts} parts done]'
        return s


class TasksHandler:
    # a class to handle selenium + url get.
    def __init__(self):
        self._jobs_to_submit = Queue()
        self._job_results_to_gather = Queue()
        self.requests_received = 0

    def add_job_to_parse(self, req: Request, *args, **kwargs):
        self._jobs_to_submit.put({'request': req})
        self.requests_received += 1

    def get_jobs_to_submit(self):
        return self._jobs_to_submit

    def get_job_results(self):
        return self._job_results_to_gather


class RequestsHandler:
    def __init__(self, tasks_handler: TasksHandler, max_memory=100):
        self._tasks_handler = tasks_handler
        # combining requests
        self._available_request_ids = Queue(maxsize=max_memory)
        for i in range(max_memory):
            self._available_request_ids.put(i)

        self._request_id_data_mapper = dict()
        self._request_id_request_mapper = dict()

        self._done_requests = Queue()

        self._done = False
        self.requests_completed = 0

        self._thread_worker = None

    def add_request(self, req: Request):
        if not self._available_request_ids.empty():
            req_id = self._available_request_ids.get()
            req.parent_idx = req_id
            self._request_id_request_mapper[req_id] = req
            self._tasks_handler.add_job_to_parse(req)
        else:
            raise MaxMemoryLimit('max amount of requests parsing already')

    def free_request_id(self, req_id: int):
        self._request_id_data_mapper.pop(req_id, None)
        self._request_id_request_mapper.pop(req_id, None)
        self._available_request_ids.put(req_id)

    def get_request_id_data_mapper(self):
        return self._request_id_data_mapper

    def gather_results(self):
        while not self._done:
            q = self._tasks_handler.get_job_results()
            while not q.empty():
                job_result = q.get()
                returned_request = job_result['request']
                # print('returned', returned_request.parent_idx, returned_request.args)
                real_request = self._request_id_request_mapper[returned_request.parent_idx]
                # print('real', real_request.parent_idx, real_request.args)
                container = self._request_id_data_mapper.get(returned_request.parent_idx, RequestData())
                # because i change only parent idx and do not change args kwargs
                # making sure i remove right request
                job_result['request'] = real_request
                # print('changed to', real_request.parent_idx, real_request.args)
                container.add_data(job_result)
                if container.is_completed():
                    # print(f'request {real_request.parent_idx} completed!')
                    self._done_requests.put(container)
                    self.free_request_id(returned_request.parent_idx)
                    self.requests_completed += 1

            time.sleep(1)

    def get_done_requests(self):
        return self._done_requests

    def start(self):
        self._done = False
        self._thread_worker = Thread(target=self.gather_results)
        self._thread_worker.start()

    def stop(self):
        self._done = True

    def join(self):
        self._thread_worker.join()
