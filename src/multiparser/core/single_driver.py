from threading import Thread
from queue import Queue
import queue
import threading


class SingleDriverBase:
    """
    Base class for single parser.

    Args:
        worker_num (int): Worker num
        request_tasks (Queue): Q to store requests along all workers.
        requests_handler (RequestHandler): handler to execute requests.
    """

    def __init__(self, worker_num: int, requests_handler: 'RequestsHandler'):
        self._worker_num = worker_num
        self._requests_handler = requests_handler

        self._working_thread = None

        self._done = False

    @property
    def requests_done(self) -> Queue:
        return self._requests_handler.requests_done

    def thread_worker(self):
        while not self._done:
            try:
                req_data = self._requests_handler.requests_to_do.get(timeout=1)
                req = req_data['request']

                parsed_data = req(self)
                result = {'request': req, 'data': parsed_data}

                self.requests_done.put(result)
            except queue.Empty:
                pass

    def start(self):
        print(f'SingleDriver[{self._worker_num}] started')
        self._working_thread = Thread(target=self.thread_worker)
        self._working_thread.start()

    def stop(self):
        print(f'SingleDriver[{self._worker_num}] stopped')
        self._done = True
        self.join()

    def join(self):
        self._working_thread.join()


class SingleDriver(SingleDriverBase):
    def __init__(self, worker_num: int, requests_handler: 'RequestsHandler',
                 multi_lock: threading.Lock):
        super(SingleDriver, self).__init__(worker_num, requests_handler)
        self.lock = threading.Lock()
        self.multi_lock = multi_lock

    def stop(self):
        print(f'SingleDriver[{self._worker_num}] stopped')
        self._done = True
        self.join()
