from html_parsers.search_parser import *
from thread_parser import *
from tasks_handler import *
import time
from threading import Thread
from ibapi import client, wrapper, contract
import json
import collections
import os
import csv
import pandas as pd


class TestWrapper(wrapper.EWrapper):
    def __init__(self, write_queue: 'Queue'):
        wrapper.EWrapper.__init__(self)
        self.write_queue = write_queue

    def currentTime(self, time: int):
        self._currentTime = time

    def historicalData(self, reqId: int, bar: 'BarData'):
        self.write_queue.put((reqId, bar))


class TestClient(client.EClient):
    def __init__(self, wrapper):
        client.EClient.__init__(self, wrapper)


class TestApp(TestWrapper, TestClient):
    def __init__(self, write_queue: Queue):
        TestWrapper.__init__(self, write_queue)
        TestClient.__init__(self, wrapper=self)


class App:
    def __init__(self):
        self.th = TasksHandler()
        self.mtp = MultiThreadParser(self.th, n_workers=3)
        self.rh = RequestsHandler(self.th, max_memory=10)

        self.tc = TestApp(Queue())

        self.worker = None
        self.tc_worker = None

    def run(self):
        self.mtp.start()
        self.rh.start()

    def launch_client(self):
        self.tc.connect("127.0.0.1", 7496, clientId=0)
        self.tc.run()

    def start(self):
        self.worker = Thread(target=self.run)
        self.worker.start()

        self.tc_worker = Thread(target=self.launch_client)
        self.tc_worker.start()

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
        self.tc_worker.join()


def load_contracts_info():
    with open('S:/ib/dump/exchanges/IDEALPRO/contracts.txt', 'r') as f:
        return json.load(f)


def save_data(done_req: 'RequestData'):
    data = done_req.data[0].get('data')
    req = done_req.data[0].get('request')
    ctr = req.args[1]  # contract data
    mode = req.args[-1]  # ASK BID
    end_timestamp = req.args[2]

    save_path = f'./dumps/exchanges/IDEALPRO/data/tickers/5m/{mode}/{ctr.symbol}_{ctr.currency}/{end_timestamp.strftime("%Y%m%d_%H%M%S")}.csv'
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    # print(save_path)
    with open(save_path, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'open', 'high', 'low', 'close'])
        for bar in data:
            writer.writerow(
                [bar.__getattribute__(name) for name in ['date', 'open', 'high', 'low', 'close']])


def csv_to_pd(path: 'str'):
    names = ['date', 'open', 'high', 'low', 'close']
    raw_data = pd.read_csv(path, header=0, names=names, index_col=False)
    idx = pd.to_datetime(raw_data['date'], format="%Y%m%d  %H:%M:%S")
    values = [raw_data[chart] for chart in names[1:]]
    raw_data = pd.concat(values, keys=names[1:], axis=1)
    raw_data.index = idx.values
    return raw_data

def data_to_pd(data:list):
    rows = list()
    idx = list()
    for bar in data:
        rows.append([bar.__getattribute__(name) for name in ['open', 'high', 'low', 'close']])
        idx.append(pd.to_datetime(bar.__getattribute__('date'), format="%Y%m%d  %H:%M:%S"))
    return pd.DataFrame(rows, index=idx, columns=['open', 'high', 'low', 'close'])

def append_missing_data(done_req: 'RequestData'):
    data = done_req.data[0].get('data')
    req = done_req.data[0].get('request')
    filename = req.kwargs['filename']
    stored_data = csv_to_pd(filename)
    new_data = data_to_pd(data)
    final_df = pd.concat([stored_data, new_data])
    final_df = final_df.sort_index()
    final_df = final_df.loc[~final_df.index.duplicated(keep='first')]
    with open(filename, 'w+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'open', 'high', 'low', 'close'])
        for idx in final_df.index:
            # care about KRW_USD data formatting
            writer.writerow([idx.strftime('%Y%m%d  %H:%M:%S'), *['{:.10f}'.format(x) for x in final_df.loc[idx]]])


def start_parsing(a: App, requests_to_make: collections.deque, save_fnc: callable = lambda x: None):
    total_requests = len(requests_to_make)
    total_requests_done = 0
    done_requests = list()
    while total_requests != total_requests_done:
        if len(requests_to_make):
            req = requests_to_make.popleft()
            try:
                a.add_request(req)
            except MaxMemoryLimit:
                requests_to_make.appendleft(req)  # return request back to queue

        try:
            done_req = a.get_done_requests().get(timeout=1)
            # done_requests.append(done_req)
            save_fnc(done_req)
            if total_requests_done % 10 == 0:
                print(f'done {total_requests_done}/{total_requests}')
            total_requests_done += 1
        except queue.Empty:
            pass

        time.sleep(0.1)
    return done_requests


def get_timestamps(date_from: datetime.datetime, date_to: datetime.datetime, timedelta):
    timestamps = list()
    current_timestamp = date_to
    while current_timestamp > date_from:
        timestamps.append(current_timestamp)
        current_timestamp -= timedelta  # add one day delta to connect edges of weeks.
    return timestamps


def load_missing_days(a: 'App'):
    # loading missing days to weekly data on the edge of files
    requests_to_make = collections.deque()
    contracts_info = load_contracts_info()
    exclude = [('AUD_USD', ), ('EUR_USD', )]
    for contract_info in contracts_info:
        ctr = contract.Contract()
        ctr.secType = "CASH"
        ctr.exchange = "IDEALPRO"
        ctr.conId = contract_info['conid']
        ctr.symbol = contract_info['ticker']
        ctr.currency = contract_info['currency']
        for mode in ['ASK', 'BID']:
            if (f'{ctr.symbol}_{ctr.currency}', mode) in exclude or (f'{ctr.symbol}_{ctr.currency}',) in exclude:
                continue
            path = f'S:\\newl\\trading\\ib_data_gather\\dumps\\exchanges\\IDEALPRO\\data\\tickers\\5m\\{mode}\\{ctr.symbol}_{ctr.currency}'
            for filename in sorted(os.listdir(path), reverse=True):
                end_timestamp = datetime.datetime.strptime(filename.split('.')[0],
                                                           "%Y%m%d_%H%M%S") + datetime.timedelta(days=1)
                req = Request(IBApiGetter(), a.tc, ctr, end_timestamp, '2 D', '5 mins', mode, filename=os.path.join(path, filename))
                requests_to_make.append(req)
    a.start()
    d = start_parsing(a, requests_to_make, append_missing_data)
    a.stop()


def main(a: App):
    contracts_info = load_contracts_info()
    contracts = dict()
    for contract_info in contracts_info:
        contracts['_'.join([contract_info['ticker'], contract_info['currency']])] = contract_info
    modes_info = ['ASK', 'BID']
    requests_to_make = collections.deque()

    date_to = datetime.datetime(year=2022, month=2, day=26, hour=23, minute=59, second=59)
    date_from = datetime.datetime(year=2021, month=7, day=29, hour=23, minute=59, second=59)
    timedelta = datetime.timedelta(days=6)  # due to week edges data is not clear
    timestamps = get_timestamps(date_from, date_to, timedelta)

    # starting from datetime.datetime(year=2017, month=10, day=3, hour=23, minute=59, second=59)
    # there is no USD_TRY data
    exclude = []
    for end_timestamp in timestamps:
        for contract_info in contracts_info:
            for mode in modes_info:
                ctr = contract.Contract()
                ctr.secType = "CASH"
                ctr.exchange = "IDEALPRO"
                ctr.conId = contract_info['conid']
                ctr.symbol = contract_info['ticker']
                ctr.currency = contract_info['currency']
                if ctr.symbol in exclude or ctr.currency in exclude:
                    continue
                # print(contract_info['ticker'], contract_info['currency'], end_timestamp)
                req = Request(IBApiGetter(), a.tc, ctr, end_timestamp, '1 W', '5 mins', mode)
                requests_to_make.append(req)

    # parser, *args, **kwargs. will be parser.__call__(single_parser, request, *args, **kwargs)
    a.start()
    d = start_parsing(a, requests_to_make, save_data)
    a.stop()


if __name__ == '__main__':
    a = App()

    main(a)
    # load_missing_days(a)

    # ALL THE CODE DONE COZ I CAN NOT LAUNCH MULTIPLE APPS IN DIFFERENT THREADS BROWSER-LIKE.
