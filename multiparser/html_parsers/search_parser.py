from bs4 import BeautifulSoup
import time
import datetime
import queue


class SearchParser:
    def parse(self, driver, *args, **kwargs):  # job_id in kwargs
        driver.get(url)
        return {'html': driver.page_source}

    def __call__(self, *args, **kwargs):
        return self.parse(*args, **kwargs)

    def __str__(self):
        return self.__class__.__name__


class HtmlGetter:
    def __call__(self, single_parser, url):
        single_parser.driver.get(url)
        result = {'html': single_parser.driver.page_source, 'parse_timestamp': datetime.datetime.utcnow()}
        return result


class IBApiGetter:
    def __call__(self, single_parser, request, client, contract_info, end_timestamp, request_time_len, granularity,
                 ask_bid_mode, *args, **kwargs):
        client.reqHistoricalData(request.parent_idx, contract_info, end_timestamp.strftime('%Y%m%d %H:%M:%S GMT'),
                                 request_time_len,
                                 granularity,
                                 ask_bid_mode, 0, 1,
                                 False, [])
        data = list()
        data_to_put = list()

        while 1:
            with single_parser.multi_lock:
                try:
                    request_id, request_data = client.write_queue.get(timeout=1)
                    data.append(request_data)

                    time.sleep(0.5)

                    while not client.write_queue.empty():
                        new_idx, new_data = client.write_queue.get()
                        # print(f'{single_parser._worker_num} got {new_idx} with {request_id}')
                        if new_idx != request_id:
                            data_to_put.append((new_idx, new_data))
                        else:
                            data.append(new_data)
                    for temp_data in data_to_put:
                        # print(f'{single_parser._worker_num} put {temp_data[0]} coz {request_id}')
                        client.write_queue.put(temp_data)
                    break
                except queue.Empty:
                    pass

        # print(f'changing {request.parent_idx} to {request_id}')
        request.parent_idx = request_id  # sync ids with jobs queue
        return data
