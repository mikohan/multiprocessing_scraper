import csv
import requests
from itertools import cycle
import MySQLdb
from io import open as iopen
import os, re, time
import progressbar
import sys
from random import shuffle
import configparser


class ImageDownloader():

    config = configparser.ConfigParser()
    config.read('config.cnf')
    host = config['DEFAULT']['host']
    user = config['DEFAULT']['user']
    password = config['DEFAULT']['password']
    database = config['DEFAULT']['database']
    limit = 10 #Limit for testing

    table = 'product_allegro'
    db = MySQLdb.connect(host, user, password, database)
    db.set_character_set('utf8')
    cursor = db.cursor()
    
    def __init__(self, *args):
        pass
    
    def apart_urls(self, pk, string):
        array = string.lstrip(',').split(',')
        array = [ x.strip() for x in array ]
        return([pk, array])

    def iter_row(self, size=10):
        db = MySQLdb.connect(self.host, self.user, self.password, self.database)
        db.set_character_set('utf8')
        cursor = db.cursor()
        q = f'SELECT id, subcat_id  FROM {self.table} LIMIT {self.limit}'
        cursor.execute(q)
        while True:
            rows = cursor.fetchmany(size)
            if not rows:
                break
            yield (rows)

    def proxy_list(self):            
        with open('proxy1.txt', 'r') as f:
            reader = csv.reader(f)
            proxy_list = list(reader)
            shuffle(proxy_list)
            for p in proxy_list:
                yield p[1] + ":" + p[2]

    def user_agent(self):
        with open('useragents', 'r') as f:
            reader = csv.reader(f)
            user_agents = list(reader)
            shuffle(user_agents)
            for ua in user_agents:
                yield ua[0]

    def do_image_job(self):
        proxy = next(self.proxy_list())
        user_agent = next(self.user_agent())
        for i, row in enumerate(self.iter_row(50)):
            print(row, i)

    def do_translate_job(self):
        with progressbar.ProgressBar(max_value=self.limit) as bar:    
            for i, row in enumerate(self.iter_row(2)):
                self.translator(row)
                bar.update(i)


    def translator(self, trans_list):
        proxy = next(self.proxy_list())
        user_agent = next(self.user_agent())
        url = 'https://www.webtran.ru/gtranslate/'
        for i, r in enumerate(trans_list):
            data = {'text': r[1],
                    'gfrom': 'pl',
                    'gto': 'ru',
                    'key': '781687649ru2419'
           }
            headers = {'User-Agent': user_agent}
            try:
                print('responsing')
                response = requests.post(url, data=data, headers=headers,timeout=10, proxies={'http': proxy, 'https': proxy})
                time.sleep(4)
#                    qu = f'UPDATE {self.table} SET subsubcat_id = %s WHERE id = %s'
#                    cursor.execute(qu, (response.text, r[0]))
#                    db.commit()
                print(response.text)
            except Exception as e:
                print(e)
                time.sleep(5)
                proxy = next(self.proxy_list())
                user_agent = next(self.proxy_list())
            #    print(e)
            #    time.sleep(20)
#            cursor.close()
#            db.close()


if __name__ == '__main__':
    imdow = ImageDownloader()
    ua = imdow.user_agent()
    pl = imdow.iter_row()
    print(imdow.do_translate_job())
