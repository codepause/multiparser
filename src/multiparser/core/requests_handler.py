import time

from queue import Queue
from threading import Thread

from multiparser.custom_exceptions.cex import MaxMemoryLimit
from multiparser.core.request import RequestData
from multiparser.core.speedometer import Speedometer


class _InnerQ:
    """
    Storage class for requests tracking.
    """

    def __init__(self, speedometer: Speedometer):
        self.speedometer = speedometer

        self.requests_to_do = Queue()
        self.requests_done = Queue()
        self.requests_done_data = Queue()
        self.num_requests_to_do = 0
        self.num_containers_done = 0
        self.num_requests_done = 0

    def add_request_to_do(self, req: 'Request', *_, **__):
        self.requests_to_do.put({'request': req, 'speedometer': self.speedometer})
        self.num_requests_to_do += 1

    def add_request_done_data(self, container: 'RequestData'):
        self.requests_done_data.put(container)
        self.num_containers_done += 1


class RequestsHandler:
    def __init__(self, speedometer: 'Speedometer' = None, max_memory=100):
        speedometer = speedometer or Speedometer()
        self._inner_q = _InnerQ(speedometer)
        self._available_request_ids = Queue(maxsize=max_memory)
        for i in range(max_memory):
            self._available_request_ids.put(i)
        self._request_id_data_mapper = dict()
        self._request_id_request_mapper = dict()

        self._done = False

        self._thread_worker = None

    def add_request(self, req: 'Request'):
        if not self._available_request_ids.empty():
            req_id = self._available_request_ids.get()
            req.parent_idx = req_id
            self._request_id_request_mapper[req_id] = req
            self._inner_q.add_request_to_do(req)
        else:
            raise MaxMemoryLimit('max amount of requests parsing already')

    def free_request_id(self, req_id: int):
        self._request_id_data_mapper.pop(req_id, None)
        self._request_id_request_mapper.pop(req_id, None)
        self._available_request_ids.put(req_id)

    def get_request_id_data_mapper(self) -> dict:
        return self._request_id_data_mapper

    @property
    def speedometer(self) -> Speedometer:
        return self._inner_q.speedometer

    @property
    def requests_done(self) -> Queue:
        return self._inner_q.requests_done

    @property
    def requests_to_do(self) -> Queue:
        return self._inner_q.requests_to_do

    @property
    def requests_done_data(self) -> Queue:
        return self._inner_q.requests_done_data

    @property
    def num_requests_to_do(self) -> int:
        return self._inner_q.num_requests_to_do

    @property
    def num_containers_done(self) -> int:
        return self._inner_q.num_containers_done

    @property
    def num_requests_done(self) -> int:
        return self._inner_q.num_requests_done

    @property
    def num_requests_undone(self) -> int:
        return self.num_requests_to_do - self.num_containers_done

    def gather_results(self):
        while not self._done:
            q = self.requests_done
            while not q.empty():
                request_data = q.get()
                request_done = request_data['request']

                request = self._request_id_request_mapper[request_done.parent_idx]
                container = self._request_id_data_mapper.get(request.parent_idx, RequestData())

                # in case user changed parent idx while parsing:
                request_data['request'] = request

                container.add_data(request_data)
                self._inner_q.num_requests_done += 1
                if container.is_completed():
                    self._inner_q.add_request_done_data(container)
                    self.free_request_id(request.parent_idx)

            time.sleep(0.5)

    def start(self):
        self._done = False
        self._thread_worker = Thread(target=self.gather_results)
        self._thread_worker.start()

    def stop(self):
        self._done = True

    def join(self):
        self._thread_worker.join()
