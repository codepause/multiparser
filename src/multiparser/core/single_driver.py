from threading import Thread
from queue import Queue
import queue
import threading


class SingleDriverBase:
    """
    Base class for single parser.

    Args:
        worker_num (int): Worker num
        requests_handler (RequestHandler): handler to execute requests.
        lock (Lock): Multithread lock to sync timings.
    """

    def __init__(self, worker_num: int, requests_handler: 'RequestsHandler', lock: threading.Lock):
        self._worker_num = worker_num
        self._requests_handler = requests_handler
        self._lock = lock

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
                speedometer = req_data['speedometer']

                speedometer.wait_required_time(self._lock)

                try:
                    parsed_data = req(self)
                except Exception as e:
                    print(f'SingleDriverBase[{self._worker_num}] caught {e}')
                    parsed_data = None

                result = {'request': req, 'data': parsed_data}

                self.requests_done.put(result)
            except queue.Empty:
                pass

    def start(self):
        print(f'SingleDriverBase[{self._worker_num}] started')
        self._working_thread = Thread(target=self.thread_worker)
        self._working_thread.start()

    def stop(self):
        print(f'SingleDriverBase[{self._worker_num}] stopped')
        self._done = True
        self.join()

    def join(self):
        self._working_thread.join()
