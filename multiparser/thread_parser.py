from threading import Thread
from queue import Queue
import queue
from selenium import webdriver
import time
import datetime
from copy import deepcopy
from tasks_handler import Request
import threading


class SingleParserBase:
    def __init__(self, worker_num: int, request_tasks: Queue, request_tasks_done: Queue):
        self._worker_num = worker_num
        self._request_tasks = request_tasks  # put in there more tasks from request. i.e. more pages?
        self._request_tasks_done = request_tasks_done

        self._working_thread = None

        self._done = False

    def thread_worker(self):
        while not self._done:
            try:
                params = self._request_tasks.get(timeout=1)
                req = params['request']
                # print(f'Worker {self._worker_num} with {params}')

                parsed_data = req(self)
                result = {'request': req, 'data': parsed_data}

                self._request_tasks_done.put(result)
            except queue.Empty:
                pass

    def start(self):
        print(f'SingleParser[{self._worker_num}] started')
        self._working_thread = Thread(target=self.thread_worker)
        self._working_thread.start()

    def stop(self):
        print(f'SingleParser[{self._worker_num}] stopped')
        self._done = True
        self.join()

    def join(self):
        self._working_thread.join()


class SingleParser(SingleParserBase):
    def __init__(self, worker_num: int, request_tasks: Queue, request_tasks_done: Queue, multi_lock: threading.Lock):
        super(SingleParser, self).__init__(worker_num, request_tasks, request_tasks_done)
        # self.driver = webdriver.Chrome('./utils/chromedriver.exe')
        self.lock = threading.Lock()
        self.multi_lock = multi_lock

    def stop(self):
        print(f'SingleParser[{self._worker_num}] stopped')
        self._done = True
        # self.driver.quit()
        self.join()


class MultiThreadParser:
    def __init__(self, tasks_handler: 'TasksHandler', n_workers=1):
        self._n_workers = n_workers
        self._workers = list()
        self._request_tasks = Queue()
        self._tasks_handler = tasks_handler

        self._done = False

        self._working_thread = None

        self.lock = threading.Lock()

    def spawn_workers(self):
        # spawning single chromium parsers
        for n_worker in range(self._n_workers):
            worker = SingleParser(n_worker, self._request_tasks, self._tasks_handler.get_job_results(), self.lock)
            worker.start()
            self._workers.append(worker)

    def get_request_tasks(self):
        return self._request_tasks

    def kill_workers(self):
        # all done, exit
        for idx, worker in enumerate(self._workers):
            worker.stop()

    def _run(self, *args, **kwargs):
        # parsing jobs in a cycle
        while not self._done:
            self.submit_tasks_once()
            time.sleep(1)

    def _run_once(self, *args, **kwargs):
        self.submit_tasks_once()

    def submit_tasks_once(self):
        q = self._tasks_handler.get_jobs_to_submit()
        while not q.empty():
            self._request_tasks.put(q.get())

    def start(self, *args, **kwargs):
        print('MultiThreadParser started')
        self._done = False
        self._working_thread = Thread(target=self._run, args=args, kwargs=kwargs)
        self._working_thread.start()
        self.spawn_workers()

    def start_once(self, *args, **kwargs):
        print('MultiThreadParser started once')
        self._working_thread = Thread(target=self._run_once, args=args, kwargs=kwargs)
        self._working_thread.start()

    def stop(self):
        print('MultiThreadParser stopped')
        self._done = True
        self.kill_workers()
        self.join()

    def join(self):
        self._working_thread.join()
