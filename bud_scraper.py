import re
import codecs
import logging
import json
import threading
from collections import OrderedDict

import BeautifulSoup as bs
import requests

logging.getLogger().setLevel(logging.INFO)

class EndOfDataException(Exception):
    pass

def _address_from_map(uri):
    """error handling covered in outer scope
    """
    res = requests.get(uri)
    if res:
        return bs.BeautifulSoup(re.search("setAddress\(\"([^\)]+)\"\)", res.content).group(1),
                                convertEntities=bs.BeautifulSoup.HTML_ENTITIES).text
    return ""


class BudScraper(object):
    URL_PATTERN = "http://budownictwo.pl/%(keyword)s/firma,%(page_num)d.html"
    DATA_EXTRACT = OrderedDict({
        "name": lambda x: x.find('h2').find('a').text,
        "phone": lambda x: x.find('li', {'class': 'tel'}).text,
        "www": lambda x: x.find('li', {'class': 'www hidden-phone'}).find('a')['href'],
        "short_address": lambda x: x.find('li', {'class': 'address'}).text,
        "long_address": lambda x: _address_from_map(x.find('a', {'class': 'lead lead-cs_hl_hit_map'})['href']),
    })

    EXPORT_FORMATS = ['csv', 'json']

    def __init__(self, search_keyword):
        self._keyword = search_keyword
        self._results = []
        self._pool = []

    def fetch_data(self, num_pages=1, num_threads=1):
        num_threads = min(num_threads, num_pages)
        self._pool = []
        for i in xrange(num_threads):
            self._pool.append(threading.Thread(target=self._fetch_data,
                                               args=(num_pages, i+1)))

        for th in self._pool:
            th.start()
            
        for th in self._pool:
            th.join()


    def _fetch_data(self, max_page=1, index_inc=1):
        i = index_inc
        while True:
            if max_page and i > max_page:
                break
            url = self.URL_PATTERN % {
                "keyword": self._keyword,
                "page_num": i,
            }
            res = requests.get(url)
            if res:
                try:
                    logging.info("Parsing page %s", url)
                    self._parse_page(res.content)
                    logging.info("Entries after page: %s", len(self._results))
                    i += len(self._pool)
                except EndOfDataException:
                    break
                except Exception as ex:
                    logging.exception(ex)
                    break
            else:
                logging.error("Error HTTP: %s", res.status)



    def _parse_page(self, page_data):
        page = bs.BeautifulSoup(page_data,
                                convertEntities=bs.BeautifulSoup.HTML_ENTITIES)
        elements_processed = False
        for company_data in page.findAll('div', {'class': 'wrapper'}):
            elements_processed = True
            res = {}
            for attrib, extract_fn in self.DATA_EXTRACT.items():
                try:
                    res[attrib] = unicode(extract_fn(company_data))
                except Exception as ex:
                    logging.error("Error extracting %s, %s", attrib, ex)
            self._results.append(res)

        if not elements_processed:
            raise EndOfDataException()

    ########## export tools ############

    def _csv_export(self, filename):
        with codecs.open(filename, 'wt', 'utf-8') as f:
            f.write("%s\n" % ",".join(self.DATA_EXTRACT.keys()))
            for row in self._results:
                try:
                    f.write("%s\n" % ",".join(['"%s"' % row.get(key, '') for key in
                                               self.DATA_EXTRACT.keys()]))
                except Exception as ex:
                    logging.exception(ex)


    def _json_export(self, filename):
        with codecs.open(filename, 'wt', 'utf-8') as f:
            f.write(json.dumps(self._results))

    def export(self, filename, format="csv"):
        if format.lower() not in self.EXPORT_FORMATS:
            raise NotImplementedError("Export format %s not implemented" %
                                      format)
        getattr(self, "_%s_export" % format.lower())(filename)

if __name__ == "__main__":
    bud_scrap = BudScraper(search_keyword="tartak")
    bud_scrap.fetch_data(20, 20)
    bud_scrap.export('test.csv')

