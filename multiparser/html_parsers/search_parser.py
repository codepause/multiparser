from bs4 import BeautifulSoup
import time
import datetime
import queue
import abc

from multiparser.tasks_handler import Request
from selenium.webdriver.common.by import By
from functools import partial
import logging
from selenium.common.exceptions import NoSuchElementException


class Getter(abc.ABC):
    @abc.abstractmethod
    def __call__(self, single_parser, request, *args, **kwargs):
        pass


class HtmlGetter(Getter):
    def __call__(self, single_parser, request, url, **kwargs):
        # single parser and request are default args
        new_links = ['https://pikabu.ru', 'https://yandex.ru', 'https://google.com']

        single_parser.driver.get(url)
        for link in new_links:
            single_parser._request_handler.add_request(Request(SubHtmlGetter(request, url), link))

        result = {'html': url, 'parse_timestamp': datetime.datetime.utcnow()}
        return result


class SubHtmlGetter(Getter):
    def __init__(self, parent_request: 'Request', parent_url: 'str'):
        self.parent_url = parent_url

    def __call__(self, single_parser, request, url, **kwargs):
        result = {'html': url, 'parse_timestamp': datetime.datetime.utcnow()}
        single_parser.driver.get(url)
        return result


def decor_factory(logging_on: bool = False):
    def start_decor(fnc: callable):
        logger = None
        if logging_on:
            logger = logging.getLogger()

        fnc_name = fnc.__name__

        def wrapper(self, *args, **kwargs):
            try:
                res = fnc(self, *args, **kwargs)
            except Exception as e:
                if logger:
                    logger.warn(
                        f'SingleParser[{self.single_parser._worker_num}] caught an exception while parsing {self.search_word} in {fnc_name}: {e.__class__.__name__}')
                res = None
            return res

        return wrapper

    return start_decor


class WBSearch(Getter):
    def __init__(self, mayak: bool = True):
        self._prefix = 'https://www.wildberries.ru/catalog/0/search.aspx?sort=popular&search='
        self.mayak = mayak

    def _get_search_link(self, search_word: str) -> str:
        return self._prefix + search_word

    @decor_factory(True)
    def _get_side_mayak(self, driver, **kwargs) -> list:
        page_side = driver.find_element(By.CSS_SELECTOR, "div[class='catalog-page__side']")
        # page_size_html = page_side.get_attribute('innerHTML')
        mayak_wrapper = page_side.find_element(By.CSS_SELECTOR, "div[class='mayak-wrapper-element']")
        texts = [i.text for i in mayak_wrapper.find_elements(By.CSS_SELECTOR, 'div')[2:-1]]
        texts_formatted = [t.replace('\u202f', '') for t in texts]
        return texts_formatted

    @decor_factory(True)
    def _get_you_may_like(self, driver, **kwargs):
        may_like = driver.find_element(By.CSS_SELECTOR, "ul[class='search-tags__list j-tags-list'")
        elements = may_like.find_elements(By.CSS_SELECTOR, "li[class='search-tags__item j-tag']")
        texts = [t for ele in elements if (t := ele.text)]
        return {'texts': texts}

    @decor_factory(True)
    def _get_single_card_link(self, card, **kwargs):
        link_element = card.find_element(By.CSS_SELECTOR, "a[class='product-card__main j-card-link']")
        link = link_element.get_attribute('href')
        return link

    @decor_factory(True)
    def _get_single_card_prices(self, card, **kwargs):
        # finds all inclusions of 'price'
        price_elements = card.find_elements(By.CSS_SELECTOR, "[class*='price']")
        prices = {i.get_attribute('class'): i.text for i in price_elements}
        prices = {key: value.encode('ascii', 'ignore').decode().strip() for key, value in prices.items()}
        return prices

    @decor_factory(True)
    def _get_single_card_is_promo(self, card, driver, **kwargs):
        element = card.find_element(By.CSS_SELECTOR, "p[class='product-card__tip-promo']")
        is_promo = False
        display = element.value_of_css_property('display')
        if display == 'block':
            is_promo = True
        return is_promo

    @decor_factory(True)
    def _get_single_card_brand_name(self, card, **kwargs):
        brand_name = card.find_element(By.CSS_SELECTOR, "strong[class='brand-name']")
        return brand_name.text

    @decor_factory(True)
    def _get_single_card_goods_name(self, card, **kwargs):
        goods_name = card.find_element(By.CSS_SELECTOR, "span[class='goods-name']")
        return goods_name.text

    @decor_factory(True)
    def _get_single_card_rating_stars(self, card, **kwargs):
        rating_stars = card.find_element(By.CSS_SELECTOR, "span[class*='product-card__rating']")
        return rating_stars.get_attribute('class')

    @decor_factory(True)
    def _get_single_card_rating_count(self, card, **kwargs):
        count = card.find_element(By.CSS_SELECTOR, "span[class='product-card__count']")
        return count.text

    @decor_factory(True)
    def _get_single_card_delivery_date(self, card, **kwargs):
        delivery_date = card.find_element(By.CSS_SELECTOR, "b[class='product-card__delivery-date']")
        return delivery_date.text

    @decor_factory(True)
    def _get_single_card_delivery_type(self, card, **kwargs):
        # if you want to extract children text, provide workaround
        delivery_type = card.find_element(By.CSS_SELECTOR, "p[class='product-card__delivery']")
        return delivery_type.text

    @decor_factory(False)
    def _get_single_card_spec_action(self, card, **kwargs):
        spec_action = card.find_element(By.CSS_SELECTOR, "div[class='product-card__action']")
        return spec_action.text

    @decor_factory(True)
    def _get_single_card_mayak(self, card, **kwargs):
        mayak_wrapper = card.find_element(By.CSS_SELECTOR, "div[class='mayak-details mayak-details--in-card']")
        texts = [i.text for i in mayak_wrapper.find_elements(By.CSS_SELECTOR, 'div')]
        texts_formatted = [t.replace('\u202f', '') for t in texts]
        return texts_formatted

    def _convert_prices(self, prices: dict):
        keys = ['lower-price', 'price-old-block']
        if prices is None:
            return None
        for key in keys:
            if key in prices:
                prices[key] = prices[key].replace(' ', '')
        return prices

    def _convert_mayak(self, mayak: list):
        if mayak is None:
            return None
        new_data = dict()
        for item in mayak:
            spl = item.split(':')
            key = spl[0]
            new_data[key] = {}
            if len(spl) > 1:
                other = spl[1].strip().split(' ')
                new_data[key]['value'] = other[0]
                if len(other) > 1:
                    new_data[key]['type'] = other[1]
                    if other[2:]:
                        new_data[key]['left'] = ' '.join(other[2:])
        return new_data

    @decor_factory(True)
    def _get_single_card_data(self, card, driver, **kwargs):
        suffixes = ["link", 'prices', 'brand_name', 'goods_name', 'rating_count', 'rating_count',
                    'delivery_date', 'delivery_type', 'spec_action', 'is_promo']
        if self.mayak:
            suffixes += ['mayak']

        data = {}
        for suffix in suffixes:
            data[suffix] = getattr(self, f'_get_single_card_{suffix}')(card, driver=driver)
        self._convert_prices(data['prices'])
        if self.mayak:
            data['mayak'] = self._convert_mayak(data['mayak'])
        return data

    @decor_factory(True)
    def _get_cards(self, driver, **kwargs):
        cards = driver.find_elements(By.CSS_SELECTOR, "div[class='product-card__wrapper']")
        return cards

    def _timeout_calls(self, fnc: callable, timeout: int = 0.5, tries: int = 6, **kwargs):
        data = None
        current_tries = 0
        while data is None and current_tries < tries:
            data = fnc()
            time.sleep(timeout)
        return data

    @decor_factory(True)
    def __call__(self, single_parser, request, search_word, **kwargs):
        print(single_parser._worker_num, 'parsing', search_word)
        self.single_parser = single_parser
        self.search_word = search_word

        driver = single_parser.driver
        search_link = self._get_search_link(search_word)
        driver.switch_to.window(single_parser.driver.window_handles[0])
        driver.get(search_link)

        data = {'cards': list()}

        data['you_may_like'] = self._timeout_calls(partial(self._get_you_may_like, driver))
        if self.mayak:
            data['side_mayak'] = self._timeout_calls(partial(self._get_side_mayak, driver))
            data['side_mayak'] = self._convert_mayak(data['side_mayak'])

        cards = self._timeout_calls(partial(self._get_cards, driver))

        if cards:
            for card in cards:
                data['cards'].append(self._get_single_card_data(card, driver=driver))

        result = {
            'html': search_link,
            'parse_timestamp': datetime.datetime.utcnow().strftime("%Y/%m/%d, %H:%M:%S"),
            'data': data,
            'word': search_word
        }
        return result
