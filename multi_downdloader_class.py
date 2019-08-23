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
import multiprocessing 
from multiprocessing import Pool

config = configparser.ConfigParser()
config.read('config.cnf')
host = config['DEFAULT']['host']
user = config['DEFAULT']['user']
password = config['DEFAULT']['password']
database = config['DEFAULT']['database']
#db = MySQLdb.connect(host, user, password, database)
class ImageDownloader():

    num = 0
    #limit = 1000    #Limit for testing

    
    #cursor = db.cursor()
    
    def __init__(self, table = 'product_allegro_back2',limit=100, *args, **kwargs):
        self.table = table
        self.limit = limit
        self.prox_lst = kwargs['proxy_list']
        self.proxy_count = self.proxy_count()//3

    def proxy_count(self):
        with open(self.prox_lst, 'r') as f:
            reader = csv.reader(f)
            proxy_list = list(reader)
            return len(proxy_list)

    def apart_urls(self, pk, string):
        array = string.lstrip(',').split(',')
        array = [ x.strip() for x in array ]
        return([pk, array])

    def iter_row(self, size):
        db_iter = MySQLdb.connect(host, user, password, database)
        db_iter.set_character_set('utf8')
        cursor_iter = db_iter.cursor()
        #Getting data from table to translate
        q = f'SELECT id, subcat_id  FROM {self.table} WHERE (subsubcat_id = "" OR subsubcat_id ="None" OR\
                LENGTH(subsubcat_id) < 5) AND LENGTH(subcat_id) > 5  LIMIT 0,{self.limit}'
        cursor_iter.execute(q)
        while True:
            rows = cursor_iter.fetchmany(size)
            if not rows:
                break
            yield (rows)
        cursor_iter.close()

    def proxy_list(self):            
        with open(self.prox_lst, 'r') as f:
            reader = csv.reader(f)
            proxy_list = list(reader)
            self.proxy_count = len(proxy_list)
            shuffle(proxy_list)
            for p in proxy_list:
                yield p[0] + ":" + p[1]

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

    def do_translate_job(self, rows_list):
        #iter_rows Выбирает из бд количество строк и выдает их по пучку в каждый процесс
        with progressbar.ProgressBar(max_value=self.limit) as bar:    
            for i, row in enumerate(rows_list): #Задаем количество строк в банче
                self.translator(rows_list)
                bar.update(i)
        return(self.num)
        
    def translator(self, trans_list):
        not_inserted = 0
        print("Length of chunk: ", len(trans_list))
        time.sleep(5)
        db = MySQLdb.connect(host, user, password, database)
        db.set_character_set('utf8')
        cursor = db.cursor()
        proxy = next(self.proxy_list())
        user_agent = next(self.user_agent())
        url = 'https://www.webtran.ru/gtranslate/'
        with progressbar.ProgressBar(max_value=len(trans_list)) as bar:
            for i, r in enumerate(trans_list):
                data = {'text': r[1],
                        'gfrom': 'pl',
                        'gto': 'ru',
                        'key': '781687649ru2419'
               }
                headers = {'User-Agent': user_agent}
                try:
                    response = requests.post(url, data=data, headers=headers,timeout=10, proxies={'http': proxy, 'https': proxy})
                    time.sleep(4)

                    if len(response.text)>5:
                        text = response.text
                    else:
                        text = 'None'

                    qu = f'UPDATE {self.table} SET subsubcat_id = %s WHERE id = %s'
                    cursor.execute(qu, (text, r[0]))
                    if cursor.rowcount == 0:
                       not_inserted += 1 
                    else:
                        self.num += cursor.rowcount
                    #print(response.text)
                except Exception as e:
                    print(e)
                    time.sleep(5)
                    #rchk = requests('http://angara77.com', timeout=2)
                    #if rchk.status_code != 200:
                    proxy = next(self.proxy_list())
                    user_agent = next(self.proxy_list())
                bar.update(i)         
                db.commit()
            cursor.close()
            db.close()
        return not_inserted
def do_all():

    limit = 100000
    processes = 50
    table = 'product_allegro_back2' 
    q = f'SELECT COUNT(id) FROM {table} WHERE (subsubcat_id = "" OR subsubcat_id ="None" OR\
 LENGTH(subsubcat_id) < 5) AND LENGTH(subcat_id) > 5  LIMIT 0,{limit}'
    db2 = MySQLdb.connect(host, user, password, database)
    cursor2 = db2.cursor() 
    cursor2.execute(q)
    count = cursor2.fetchall()
    print(f"Working whith {count[0][0]} items ")
    chunk = count[0][0] // processes
    print("Size of chunk: ", chunk)
    time.sleep(10)
    cursor2.close()

    imdow = ImageDownloader(table, limit, proxy_list='proxy_23.csv')
    with Pool(processes) as p:
        var = p.map(imdow.translator, imdow.iter_row(chunk) )
#    with progressbar.ProgressBar(max_value=10) as bar:
#        for i, row in enumerate(imdow.iter_row(10)):
#            p = multiprocessing.Process(target=imdow.translator, args=(row,))
#            p.start()
#            bar.update(i)
        p.close()
        p.join()
    #db.close()
    print(f'Connection Closed', imdow.num) 
    print("Not inserted:", var)
if __name__ == '__main__':
    t1 = time.time()
    do_all() 
    t2 = time.time()
    print("Total tim :", (t2 - t1)/60, 'Minutes')
