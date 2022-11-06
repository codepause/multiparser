from bs4 import BeautifulSoup
import time
import datetime
import queue

from multiparser.tasks_handler import Request


class HtmlGetter:
    def __call__(self, single_parser, request, url):
        # single parser and request are default args
        new_links = ['https://pikabu.ru', 'https://yandex.ru', 'https://google.com']

        single_parser.driver.get(url)
        for link in new_links:
            single_parser._request_handler.add_request(Request(SubHtmlGetter(request, url), link))

        result = {'html': url, 'parse_timestamp': datetime.datetime.utcnow()}
        return result


class SubHtmlGetter:
    def __init__(self, parent_request: 'Request', parent_url: 'str'):
        self.parent_url = parent_url

    def __call__(self, single_parser, request, url):
        result = {'html': url, 'parse_timestamp': datetime.datetime.utcnow()}
        single_parser.driver.get(url)
        return result
