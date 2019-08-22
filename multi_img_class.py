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
from pathlib import Path


config = configparser.ConfigParser()
config.read('config.cnf')
host = config['DEFAULT']['host']
user = config['DEFAULT']['user']
password = config['DEFAULT']['password']
database = config['DEFAULT']['database']
db = MySQLdb.connect(host, user, password, database)
db.set_character_set('utf8')

class ImageDownloader():

    num = 0
    #limit = 1000    #Limit for testing

    
    #cursor = db.cursor()
    
    def __init__(self, **kwargs):
        self.table = kwargs['table']
        self.limit = kwargs['limit']
        self.img_format = kwargs['img_format']
        self.working_path = kwargs['working_path']
        self.img_size = kwargs['img_size']

    def proxy_count(self):
        with open('proxy.csv', 'r') as f:
            reader = csv.reader(f)
            proxy_list = list(reader)
            return len(proxy_list)

    def apart_urls(self, string):
        array = string.lstrip(',').split(',')
        array = [ x.strip() for x in array ]
        return(array)

    def iter_row(self, size):
        cursor = db.cursor()
        #Getting data from table to translate
        q = f'SELECT id, img  FROM {self.table} WHERE img !=""  AND img_check != 1 LIMIT 0,{self.limit}'
        cursor.execute(q)
        while True:
            rows = cursor.fetchmany(size)
            if not rows:
                break
            yield (rows)

    def proxy_list(self):            
        with open('proxy1.csv', 'r') as f:
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

    def do_image_job(self, chunk):
        db = MySQLdb.connect(host, user, password, database)
        db.set_character_set('utf8')
        cursor = db.cursor()
        user_agent = next(self.user_agent())
        p_l = self.proxy_list()
        proxy = next(p_l)
        proxies = {'http': proxy, 'https': proxy}
        headers = {'User-Agent': user_agent}
        with progressbar.ProgressBar(max_value=len(chunk)) as bar:
            for b, row in enumerate(chunk):
                pk, urls = row[0], self.apart_urls(row[1])
                directory = os.path.join(self.working_path, str(pk))
                if not os.path.exists(directory):
                    os.makedirs(directory)
                for i, url in enumerate(urls):
                    if '-Typ-' not in url:
                        file_name = str(i) + '.' + self.img_format
                        uri = re.sub(r's128', 's{}'.format(self.img_size), url)
                        code = False
                        while not code: 
                            try:
                                r = requests.get(uri, headers=headers, timeout=10)
                                file_name = os.path.join(self.working_path, directory, file_name)
                                with open(file_name, 'wb') as f:
                                    f.write(r.content)
                                    time.sleep(0.5)
                                #here update mage_check
                                qu = f'UPDATE {self.table} SET img_check = %s WHERE id = %s'
                                cursor.execute(qu, (1, pk))
                                db.commit()
                                if r.status_code == 200:
                                    code = True
                                    time.sleep(0.3)
                                else:
                                    try:
                                        proxy = next(p_l)
                                    except StopIteration:
                                        p_l = self.proxy_list()
                                        proxy = next(p_l)
                                        
                            except Exception as e:
                                print(e)
                                proxy = next(p_l)
                bar.update(b) 
        
        cursor.close()
        db.close()

def do_all():
   #Здесь нужно задать настройки для загрузки фотографий

    table = 'product_allegro_spec' #Задаем таблицу из которой выбираем урлы и записываем img_check = 1 
    limit = 10 # Лимит выборки из бд, если нужно выбрать все, то ставим лимит больше чем строк в таблице
    img_format = 'webp' #Формат изображения с которым работаем
    img_size = 900 #Размер картинки которую будем скачивать(работает только с аллегро, остальные надо смотреть урлы)
    processes = 4 #Количество процессов которые будут парстить
    
    chunk = limit // processes
    working_path = os.path.join(str(Path.home()),'tmp', 'img_down')
    imdow = ImageDownloader(table=table, limit=limit, img_format=img_format, working_path=working_path,img_size=img_size)
    with Pool(processes) as p:
        var = p.map(imdow.do_image_job, imdow.iter_row(chunk) )
        p.close()
        p.join()
    db.close()
    print(f'Connection Closed', imdow.num) 

if __name__ == '__main__':
    t1 = time.time()
    do_all() 
    t2 = time.time()
    print("Total tim :", (t2 - t1)/60, 'Minutes')
