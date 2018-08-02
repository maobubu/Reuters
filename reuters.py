from __future__ import division
import json
import datetime
from itertools import repeat
import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool as ThreadPool
import timeit
import collections

class news_Reuters:
    def __init__(self):
        self._second = dict()
        self._user_agent = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) '
                                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'}
        date_entry = '2014-05-14'
        year, month, day = map(int, date_entry.split('-'))
        date1 = datetime.datetime(year, month, day)
        print(date1)

    def run(self, datelist, fin):
        line = fin.strip().split(',')
        line = line[:4]
        ticker, name, exchange, MarketCap = line
        print("{} - {} - {}".format(ticker, name, exchange))
        self.contents(ticker, name, line, datelist, exchange)
        self._second.clear()

    def contents(self, ticker, name, line, datelist, exchange):
        suffix = {'AMEX': '.A', 'NASDAQ': '.O', 'NYSE': '.N'}
        url = "https://www.reuters.com/finance/stocks/company-news/" + ticker + suffix[exchange]
        has_content = 0
        repeat_times = 5
        url = self.check(ticker, url)
        # check the website to see if that ticker has many news
        # if true, iterate url with date, otherwise stop
        for _ in range(repeat_times):  # repeat in case of http failure
            try:
                # response = urllib.request.urlopen(url)
                # data = response.read()
                response = requests.get(url, headers=self._user_agent)
                data = response.text
                # print data #NEWLINEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEe
                soup = BeautifulSoup(data, 'html.parser')
                has_content = len(soup.find_all("div", {'class': ['topStory', 'feature']}))
                break
            except Exception as e:
                print("Content exception:{}".format(str(e)))
                with open('exception.txt', "a+") as f:
                    f.write("Content exception:{}".format(str(e)))
                continue
        ticker_failed = open('news_failed_tickers.csv', 'a+')
        if has_content >= 0:
            missing_days = 0
            for timestamp in datelist:
                hasnews = self.repeatdownload(ticker, line, url, timestamp)
                if hasnews:
                    missing_days = 0  # if get news, reset missing_days as 0
                else:
                    missing_days += 1
                if missing_days > 300:  # 2 NEWS: wait 30 days and stop, 10 news, wait 70 days
                    break  # no news in X consecutive days, stop crawling
                if missing_days > 0 and missing_days % 20 == 0:  # print the process
                    print("{} has no news for {} days, stop this candidate ...".format(ticker, missing_days))
                    ticker_failed.write(ticker + ',' + timestamp + ',' + 'LOW\n')
        else:
            print("{} has no news".format(ticker))
            today = datetime.datetime.today().strftime("%Y%m%d")
            ticker_failed.write(ticker + ',' + today + ',' + 'LOWEST\n')
        ticker_failed.close()
        with open('reuters/' + ticker + '.json', 'w') as fout:
            json.dump(self._second, fout, indent=2)

    def repeatdownload(self, ticker, line, url, timestamp):
        new_time = timestamp[4:] + timestamp[:4]  # change 20151231 to 12312015 to match reuters format
        repeat_times = 5  # repeat downloading in case of http error
        for _ in range(repeat_times):
            try:
                response = requests.get(url + "?date=" + new_time, headers=self._user_agent)
                data = response.text
                # response = urllib.request.urlopen(url + "?date=" + new_time)
                # data = response.read()
                # print data #############NEWWLINEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE
                soup = BeautifulSoup(data, 'html.parser')
                hasnews = self.parser(soup, line, ticker, timestamp)
                if hasnews:
                    return 1  # return if we get the news
                break  # stop looping if the content is empty (no error)
            except Exception as e:
                print("Repeat Download exception:{},{},{}".format(str(e), ticker, timestamp))
                with open('exception.txt', "a+") as f:
                    f.write("Repeat Download exception:{},{},{}".format(str(e), ticker, timestamp))
                continue
        return 0

    def parser(self, soup, line, ticker, timestamp):
        content = soup.find_all("div", {'class': ['topStory', 'feature']})  # WE NEED THE ENTIRE TEXT!
        if len(content) == 0:
            return 0
        first = list()
        for i in range(len(content)):
            d = dict()
            d["symbol"] = ticker
            d["company_name"] = line[1]
            d["date"] = timestamp
            title = content[i].h2.get_text().replace("\n", " ")
            abstract = content[i].p.get_text().replace("\n", " ")
            body = self.article(content[i])  # GET THE ENTIRE BODY
            d["title"] = title
            d["abstract"] = abstract
            d["article"] = body
            if i == 0 and len(soup.find_all("div", class_="topStory")) > 0:
                news_type = 'topStory'
            else:
                news_type = 'normal'
            d["news_type"] = news_type
            first.append(d)
            o_data = collections.OrderedDict(d)
            with open('reuters_new/'+ticker+'.json', 'a+') as f:
                json.dump(o_data, f)
                f.write('\r')
            self._second[timestamp] = first
            print(ticker, timestamp, title, news_type)
        return 1

    def article(self, content):
        repeat_times = 5  # repeat downloading in case of http error
        final = 'HTTP ERROR, THERE IS NO ARTICLE'
        for _ in range(repeat_times):
            try:
                body = list()
                article = content.find('a').get('href')
                url = 'https://www.reuters.com' + article
                response = requests.get(url, headers=self._user_agent)
                data = response.text
                # response = urllib.request.urlopen(url)
                # data = response.read()
                # print data #NEWLINEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEe
                soup = BeautifulSoup(data, 'html.parser')
                information = soup.find_all("p", {'class': ['MegaArticleBody_first-p_2htdt', '']})
                for i in range(len(information)):
                    body.append(information[i].get_text())
                final = ''.join(body)
                break
            except Exception as e:
                print("Article exception:{},{}".format(str(e), url))
                with open('exception.txt', "a+") as f:
                    f.write("Article exception:{},{}".format(str(e), url))
                continue
        return final

    def check(self, ticker, url):
        for _ in range(2):  # repeat in case of http failure
            try:
                response = requests.get(url, headers=self._user_agent)
                data = response.text
                # response = urllib.request.urlopen(url)
                # data = response.read()
                soup = BeautifulSoup(data, 'html.parser')
                if len(soup.find_all("div", {'class': 'no-result'})) != 0:
                    url = "https://www.reuters.com/finance/stocks/company-news/" + ticker + '.A'
                    continue
                if len(soup.find_all("div", {'class': 'no-result'})) == 0:
                    break
                else:
                    url = "https://www.reuters.com/finance/stocks/company-news/" + ticker + '.N'
                    continue
            except Exception as e:
                print('check', str(e))
                continue
        return url


def dateGenerator(numdays, base=datetime.datetime.today()):  # generate N days until now
    date_list = [base - datetime.timedelta(days=x) for x in range(0, numdays)]
    for i in range(len(date_list)):
        date_list[i] = date_list[i].strftime("%Y%m%d")
    return date_list


def main():
    start = timeit.default_timer()
    name, ruozhi = list(), list()
    datelist = dateGenerator(30)  # look back on the past X days
    with open('tickerList.csv', 'r') as f:
        for line in f:
            name.append(line)
    ruozhi = [datelist] * len(name)
    pool = ThreadPool(10)
    news = news_Reuters()
    pool.starmap(news.run, zip(ruozhi, name))
    pool.close()
    pool.join()
    stop = timeit.default_timer()
    print(stop - start)


if __name__ == "__main__":
    main()
