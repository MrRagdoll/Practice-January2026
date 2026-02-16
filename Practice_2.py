import requests
from zipfile import ZipFile
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import os
import psycopg2
from lxml import etree

#Раздел 1. Получение ссылок с сайта

def getreq (parslink, headers):
    # получение запроса по ссылке
    try:
        r = requests.get(parslink, headers=headers, allow_redirects=True, timeout=15, verify=True)
        print(f'Код подключения: {r.status_code}')
    # вывод ошибок при отсутствии соединения
    except requests.exceptions.TooManyRedirects:
        print("Ошибка: TooManyRedirects")
    except requests.exceptions.ReadTimeout:
        print("Ошибка: ReadTimeout")
    except requests.exceptions.SSLError:
        print("Ошибка: SSLError")
    except requests.exceptions.ConnectionError:
        print("Ошибка: ConnectionError")
    else:
        #ссылки на набор и структуру данных
        xml_link = None
        xsd_link = None
        # конвертация запроса в текст
        s = r.text
        # вывод получченной структуры
        soup = BeautifulSoup(s, 'html.parser')
        soup = soup.find_all("tr")
        #нахождение ссылок по их номерам в структуре
        for tr in soup:
            soup2 = tr.find_all("td")
            for td in soup2:
                if td.get_text() =="8":
                    soup3 = tr.find_all("a")
                    for a in soup3:
                        xml_link = (a.get_text('href'))
                if td.get_text() == "10":
                    soup3 = tr.find_all("a")
                    for a in soup3:
                        xsd_link = (a.get_text('href'))
        #получение ссылок
        return xml_link, xsd_link

#Раздел 2. Загрузка файлов по ссылкам

def download_file(url, save_path=None):
    #имя файла, полученное по ссылке на его загрузку
    save_path = os.path.basename(url)
    #параметр stream загружает контент по частям
    response = requests.get(url, stream=True)
    #вызов исключения при ошибке
    response.raise_for_status()
    #получение информации о размере файла
    total_size = int(response.headers.get('content-length', 0))
    #порция для загрузки в память
    block_size = 1024
    #переменная для загруженных данных
    downloaded = 0

    print(f"Файл будет загружен: {save_path}")
    with open(save_path, 'wb') as file:
        for data in response.iter_content(block_size):
            #запись данных в открытый файл
            file.write(data)
            #увеличение переменной на длину загруженных данных
            downloaded += len(data)
        #проверка на наличие данных о размере файла
        if total_size > 0:
            #процент загрузки
            progress = downloaded / total_size * 100
            #полоска загрузки
            print(f"\rПрогресс: [{downloaded}/{total_size} байт] {progress:.1f}%", end="")
    print("\nФайл загружен!")
    return save_path

#переменные для загрузки данных с сайта
# говорим веб-серверу, что хотим получить html
st_accept = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"
# формируем хеш заголовков
headers = {
   "Accept": st_accept,
   "User-Agent": UserAgent().random #генерация фальшивой мета-информации для избежания обрыва соединения
}
#ссылка на сайт
sitelink = "https://www.nalog.gov.ru/opendata/7707329152-debtam/"

#загрузка файлов
commit = input("Загрузить архив с записями? (Y - Да / N - Нет): ")
if (commit == 'Y' or commit == 'y'):
    download_file(getreq(sitelink, headers)[0],None)
    download_file(getreq(sitelink, headers)[1],None)

#Раздел 3. Получение данных из загруженного архива
commit = input("Распаковать архив с записями? (Y - Да / N - Нет): ")
if (commit == 'Y' or commit == 'y'):
    with ZipFile("../venv/data-20251225-structure-20181201.zip", "r") as myzip:
        myzip.extractall(path="../venv/entries")
#получение названий файлов
entries = os.listdir('../venv/entries')
#количество файлов
num = len(entries)

#параметр проверяет документ на ошибки
parser = etree.XMLParser(dtd_validation=True)

#подключение к базе данных
conn = psycopg2.connect(database="debt", host="localhost", user="postgres", password="12345", port="5433")
print("Подключение установлено")

#переменные для подсчёта общего количества записей предприятий
SUMM = 0
#переменная для подсчёта повторных записей
REPEAT = 0
#переменная для проверки количества записей на соответствие заявленному в файле
DocCount = 0
#переменная для поиска ИНН 1644003838
TINcheck = False

#автоматическое выполнение запросов к базе данных
conn.autocommit = True
#на случай возникновения ошибок
commit = input("Обнулить базу данных? (Y - Да / N - Нет): ")
if (commit == 'Y' or commit == 'y'):
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE СведНедоим, СведНП RESTART IDENTITY")
    cursor.close()

#Раздел 4. Загрузка записей в базу данных
commit = input("Загрузить записи в базу данных? (Y - Да / N - Нет): ")
if (commit == 'Y' or commit == 'y'):
    #цикл для файлов
    i = 0
    while (i<num):
        # Загрузить файлы XML и XSD
        xml_file = (f'../venv/entries/{entries[i]}')
        xsd_file = ('../venv/structure-20181201.xsd')
        xml_root = etree.parse(xml_file)
        xmlschema = etree.XMLSchema(file=xsd_file)
        #проверка на соответствие файла xsd-схеме
        is_valid = xmlschema.validate(xml_root)
        print(is_valid)
        #поиск записей в файле
        soup = BeautifulSoup(etree.tostring(xml_root, encoding='unicode'), 'xml')
        КолДок = soup.Файл['КолДок']
        print(f"Количество документов в файле: {КолДок}")
        soup2 = soup.find_all('Документ')
        #цикл для записей предприятий
        j=0
        while (j<len(soup2)):
            try:
                cursor = conn.cursor()
                ИННЮЛ = str(soup2[j].СведНП['ИННЮЛ'])
                НаимОрг = soup2[j].СведНП['НаимОрг']
                #запись данных о предприятиях
                try:
                    cursor.execute(f"INSERT INTO СведНП (НаимОрг, ИННЮЛ) VALUES ($${НаимОрг}$$, {ИННЮЛ})")
                except:
                    cursor.execute(f"UPDATE СведНП  SET НаимОрг = $${НаимОрг}$$ WHERE ИННЮЛ = '{ИННЮЛ}'")
                    REPEAT = REPEAT+1
                #добавляем все записи о задолженностях по предприятию
                for k in soup2[j].find_all('СведНедоим'):
                    НаимНалог = k['НаимНалог']
                    ОбщСумНедоим = k['ОбщСумНедоим']
                    СумНедНалог = k['СумНедНалог']
                    СумПени = k['СумПени']
                    СумШтраф = k['СумШтраф']
                    cursor = conn.cursor()
                    try:
                        cursor.execute(f"INSERT INTO СведНедоим (НаимНалог, СумНедНалог, СумПени, СумШтраф, ИНН) VALUES ('{НаимНалог}', {СумНедНалог}, {СумПени}, {СумШтраф}, {ИННЮЛ})")
                    except:
                        cursor.execute(f"UPDATE СведНедоим SET СумНедНалог = {СумНедНалог}, СумПени = {СумПени}, СумШтраф = {СумШтраф} WHERE ИНН = {ИННЮЛ} AND НаимНалог = {НаимНалог}")
                #если найден требуемый ИНН
                if (soup2[j].СведНП['ИННЮЛ'] == "1644003838"):
                    TINcheck = True
                cursor.close()
            except:
                print("Ошибка")
            finally:
                j=j+1
                SUMM = SUMM+1
                #если количество записей соответствует заявленному
                if (int(КолДок) == int(j)):
                    DocCount = DocCount+1
        i= i+1
        print(f"Номер итерации: {i}")
    if TINcheck == True:
        print ("ИНН найден")
    print(f"Сумма записей предприятий во всех файлах: {SUMM}")
    print(f"Сумма повторных записей в файлах: {REPEAT}")
    print(f"Соответствие записей предприятий их количеству в файле: {DocCount}")
    cursor.close()

#Раздел 5. Поиск задолженности по ИНН
commit = input("Выполнить поиск задолженности по ИНН? (Y - Да / N - Нет): ")
if (commit == 'Y' or commit == 'y'):
    cursor = conn.cursor()
    TIN = str(input("Введите искомый ИНН: "))
    cursor.execute(f"SELECT * FROM СведНедоим WHERE ИНН = '{TIN}'")
    for company in cursor:
        print(f"\nВид задолженности: {company[0]}")
        print(f"Сумма недоимки по налогу: {company[1]}", end=" ")
        print(f"Сумма пени: {company[2]}", end=" ")
        print(f"Сумма штрафа: {company[3]}", end=" ")
        print(f"Общая сумма задолженности: {company[1]+company[2]+company[3]}")
    cursor.close()
conn.close()
