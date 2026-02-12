import json
import requests
import numpy as np
import pandas as pd
from zipfile import ZipFile
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import time
import re
import os
import psycopg2
import chardet
from io import StringIO, BytesIO
from lxml import etree

#Раздел 1. Скачивание с сайта, разархивация

def getreq (parslink, headers):
    # получение запроса по ссылке
    try:
        soup = None
        r = requests.get(parslink, headers=headers, allow_redirects=True, timeout=15, verify=True)
        print(f'Код подключения: {r.status_code}')
    except requests.exceptions.TooManyRedirects:
        print("Ошибка: TooManyRedirects")
    except requests.exceptions.ReadTimeout:
        print("Ошибка: ReadTimeout")
    except requests.exceptions.SSLError:
        print("Ошибка: SSLError")
    except requests.exceptions.ConnectionError:
        print("Ошибка: ConnectionError")
    else:
        print("Goood")




st_accept = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"
# говорим веб-серверу, что хотим получить html
# имитируем подключение через браузер Mozilla на macOS
#st_useragent =["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36","Mozilla/5.0 (Macintosh; Intel Mac OS X 12_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15"]

# формируем хеш заголовков
headers = {
   "Accept": st_accept,
   "User-Agent": UserAgent().random #генерация фальшивой мета-информации для избежания обрыва соединения
}
#webbrowser.open(parslink)

sitelink = "https://www.nalog.gov.ru/opendata/7707329152-uid/"

#getreq(sitelink, headers)

#Раздел 2. Получение данных из скачанного архива

commit = input("Распаковать архив с записями? Y/N")
if (commit == 'Y'):
    with ZipFile("../data/data-20251225-structure-20181201.zip", "r") as myzip:
        myzip.extractall(path="../data/entries")
entries = os.listdir('../data/entries')
num = len(entries)
parser = etree.XMLParser(dtd_validation=True)
i = 0
while (i < num):
    xml_file = (f'../data/entries/{entries[i]}')
    xsd_file = ('../data/structure-20181201.xsd')
    i=i+1
conn = psycopg2.connect(database="debt", host="localhost", user="postgres", password="12345", port="5433")
print("Подключение установлено")
i = 0
SUMM = 0
REPEAT = 0
DocCount = 0
TINcheck = False
commit = input("Загрузить записи в базу данных? Y/N")
if (commit == 'Y'):
    while (i<num):
        # Загрузить файлы XML и XSD
        xml_file = (f'./data/data-20251225-structure-20181201/{entries[i]}')
        xsd_file = ('./data/structure-20181201.xsd')

        xml_root = etree.parse(xml_file)
        xmlschema = etree.XMLSchema(file=xsd_file)
        is_valid = xmlschema.validate(xml_root)



        soup = BeautifulSoup(etree.tostring(xml_root, encoding='unicode'), 'xml')
        КолДок = soup.Файл['КолДок']
        print(f"Количество документов в файле: {КолДок}")
        soup2 = soup.find_all('Документ')
        #print (soup2)
        j=0
        while (j<len(soup2)):
            try:
                conn.autocommit = True
                cursor = conn.cursor()
                ИННЮЛ = str(soup2[j].СведНП['ИННЮЛ'])
                НаимОрг = soup2[j].СведНП['НаимОрг']
                try:
                    cursor.execute(f"INSERT INTO СведНП (НаимОрг, ИННЮЛ) VALUES ($${НаимОрг}$$, {ИННЮЛ})")
                    #print("Не повтор")
                except:
                    cursor.execute(f"UPDATE СведНП  SET НаимОрг = $${НаимОрг}$$ WHERE ИННЮЛ = '{ИННЮЛ}'")
                    #print("Повтор")
                for k in soup2[j].find_all('СведНедоим'):
                    НаимНалог = k['НаимНалог']
                    ОбщСумНедоим = k['ОбщСумНедоим']
                    СумНедНалог = k['СумНедНалог']
                    СумПени = k['СумПени']
                    СумШтраф = k['СумШтраф']
                    conn.autocommit = True
                    cursor = conn.cursor()
                    try:
                        cursor.execute(f"INSERT INTO СведНедоим (НаимНалог, СумНедНалог, СумПени, СумШтраф, ИНН) VALUES ('{НаимНалог}', {СумНедНалог}, {СумПени}, {СумШтраф}, {ИННЮЛ})")
                    except:
                        cursor.execute(f"UPDATE СведНедоим SET СумНедНалог = {СумНедНалог}, СумПени = {СумПени}, СумШтраф = {СумШтраф} WHERE ИНН = {ИННЮЛ} AND НаимНалог = {НаимНалог}")
                if (soup2[j].СведНП['ИННЮЛ'] == "1644003838"):
                    TINcheck = True
                #cursor.execute(f"TRUNCATE TABLE Сведения RESTART IDENTITY;")
                cursor.close()
                if TINcheck == True:
                    print ("Super Good")
            except:
                REPEAT = REPEAT+1
            finally:
                j=j+1
                SUMM = SUMM+1
                if (int(КолДок) == int(j)):
                    DocCount = DocCount+1
        i= i+1
        print(f"Номер итерации: {i}")
    if TINcheck == True:
        print ("Vse Super")
    print(f"Сумма записей: {SUMM}")
    print(f"Сумма повторных записей: {REPEAT}")
    print(f"Соответствие записей их количеству: {DocCount}")
cursor = conn.cursor()
TIN = str(input("Введите искомый ИНН: "))
cursor.execute(f"SELECT * FROM СведНедоим WHERE ИНН = '{TIN}'")
for company in cursor:
    print(f"{company[0]}")
    print(f"{company[1]}")
    print(f"{company[2]}")
    print(f"{company[3]}")
    print(f"Общая сумма: {company[1]+company[2]+company[3]}")

cursor.close()
conn.close()