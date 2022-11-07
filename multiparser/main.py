from html_parsers.search_parser import *
from thread_parser import *
from tasks_handler import *
import time
from threading import Thread
from functools import partial
from selenium.webdriver.chrome.options import Options
import json
import collections
import os
import csv
import pandas as pd
from selenium.webdriver.common.by import By
import logging

logging.getLogger().setLevel(logging.DEBUG)

from thread_parser import SingleParser
from multiparser.html_parsers.search_parser import WBSearch


class SingleParserDriver(SingleParser):
    def __init__(self, *args, **kwargs):
        super(SingleParserDriver, self).__init__(*args, **kwargs)
        options = webdriver.ChromeOptions()
        options.add_extension('../temp/extension_7_3_1_0.crx.crx')
        self.driver = webdriver.Chrome(options=options)
        self._window_name = None
        self._login_to_mayak()

    def _login_to_mayak(self):
        time.sleep(1)
        h = self.driver.window_handles[1]
        self.driver.switch_to.window(h)
        time.sleep(1)
        self.driver.get('https://app.mayak.bz/users/sign_in')
        time.sleep(1)
        login = self.driver.find_element(By.ID, 'login')
        password = self.driver.find_element(By.ID, 'user_password')
        login.send_keys('a3yrsnn@gmail.com')
        password.send_keys('Nikenike1.')
        button = self.driver.find_element(By.NAME, 'commit')
        button.click()
        self.driver.close()

    @property
    def window_name(self):
        return self._window_name

    @window_name.setter
    def window_name(self, item):
        print(self._worker_num, 'window set to', item)
        self._window_name = item


class App:
    def __init__(self):
        self.th = TasksHandler()

        self.rh = RequestsHandler(
            self.th,
            max_memory=10
        )
        self.mtp = MultiThreadParser(
            self.rh,
            n_workers=1,
            parser_instance=partial(SingleParserDriver)
        )

        self.worker = None

    def run(self):
        self.mtp.start()
        self.rh.start()

    def start(self):
        self.worker = Thread(target=self.run)
        self.worker.start()

    def stop(self, wait=True):
        tasks = self.mtp.get_request_tasks()
        jobs = self.th.get_jobs_to_submit()
        reqs = self.rh.get_request_id_data_mapper()
        while (not tasks.empty() or not jobs.empty() or len(reqs) != 0) and wait:
            # print(not tasks.empty(), not jobs.empty(), len(reqs) != 0)
            time.sleep(1)
        self.mtp.stop()
        self.rh.stop()
        self.join()

    def add_request(self, req: 'Request'):
        self.rh.add_request(req)

    def get_done_requests(self):
        return self.rh.get_done_requests()

    def join(self):
        self.worker.join()


def start_parsing(a: App, requests_to_make: list):
    total_requests_done = 0
    done_requests = list()
    for req in requests_to_make:
        a.add_request(req)

    while a.th.requests_received != a.rh.requests_completed:
        try:
            done_req = a.get_done_requests().get(timeout=1)
            done_req.save()
        except queue.Empty:
            pass
        print(f'Completed {a.rh.requests_completed} out of {a.th.requests_received}')
        time.sleep(0.1)
    return done_requests


def main(a: App):
    getter = WBSearch()
    dq = [
        Request(getter, 'когтеточка'),
    ]

    a.start()
    d = start_parsing(a, dq)
    time.sleep(2)
    a.stop()


if __name__ == '__main__':
    a = App()

    main(a)
