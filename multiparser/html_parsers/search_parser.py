from bs4 import BeautifulSoup
import time
import datetime
import queue
import abc

from multiparser.tasks_handler import Request
from selenium.webdriver.common.by import By
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
                    logger.debug(
                        f'SingleParser[{self.single_parser._worker_num}] caught an exception while parsing {self.search_word} in {fnc_name}: {e}')
                res = None
            return res

        return wrapper

    return start_decor


class WBSearch(Getter):
    def __init__(self):
        self._prefix = 'https://www.wildberries.ru/catalog/0/search.aspx?sort=popular&search='
        self.cards_tries = 10
        self.cards_timeout = 1

    def _get_search_link(self, search_word: str) -> str:
        return self._prefix + search_word

    @decor_factory(True)
    def _get_side_mayak(self, driver, **kwargs):
        page_side = driver.find_element(By.CSS_SELECTOR, "div[class='catalog-page__side']")
        # page_size_html = page_side.get_attribute('innerHTML')
        mayak_wrapper = page_side.find_element(By.CSS_SELECTOR, "div[class='mayak-wrapper-element']")
        texts = [i.text for i in mayak_wrapper.find_elements(By.CSS_SELECTOR, 'div')[2:-1]]
        texts_formatted = [t.replace('\u202f', '') for t in texts]
        return {'texts': texts_formatted}

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
    def _get_single_card_is_promo(self, card, **kwargs):
        is_promo = False
        try:
            card.find_element(By.CSS_SELECTOR, "p[class='product-card__tip-promo']")
            is_promo = True
        except NoSuchElementException:
            pass
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

    @decor_factory(True)
    def _get_single_card_spec_action(self, card):
        spec_action = card.find_element(By.CSS_SELECTOR, "div[class='product-card__action']")
        return spec_action.text

    @decor_factory(True)
    def _get_single_card_mayak(self, card):
        mayak_wrapper = card.find_element(By.CSS_SELECTOR, "div[class='mayak-details mayak-details--in-card']")
        texts = [i.text for i in mayak_wrapper.find_elements(By.CSS_SELECTOR, 'div')]
        texts_formatted = [t.replace('\u202f', '') for t in texts]
        return texts_formatted

    @decor_factory(True)
    def _get_single_card_data(self, card, **kwargs):
        suffixes = ["link", 'prices', 'brand_name', 'goods_name', 'rating_count', 'delivery_date',
                    'delivery_type', 'spec_action', 'mayak']

        data = {}
        for suffix in suffixes:
            data[suffix] = getattr(self, f'_get_single_card_{suffix}')(card)

        return data

    @decor_factory(True)
    def _get_cards(self, driver, **kwargs):
        cards = driver.find_elements(By.CSS_SELECTOR, "div[class='product-card__wrapper']")
        return cards

    @decor_factory(True)
    def __call__(self, single_parser, request, search_word, **kwargs):
        self.single_parser = single_parser
        self.search_word = search_word

        driver = single_parser.driver
        search_link = self._get_search_link(search_word)
        driver.switch_to.window(single_parser.driver.window_handles[0])
        driver.get(search_link)

        cards = list()
        tries_counter = 0
        while not cards and tries_counter < self.cards_tries:
            cards = self._get_cards(driver)
            time.sleep(self.cards_timeout)
        data = {'cards': list()}
        data['side_mayak'] = self._get_side_mayak(driver)
        data['you_may_like'] = self._get_you_may_like(driver)
        if cards:
            for card in cards:
                data['cards'].append(self._get_single_card_data(card))

        result = {'html': search_link, 'parse_timestamp': datetime.datetime.utcnow(), 'data': data}
        return result
