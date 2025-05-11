import logging
from datetime import datetime
import requests
import sqlite3
from bs4 import BeautifulSoup
from time import sleep
import random
from typing import Optional, Union
import re
from urllib.parse import quote
from config import BASE_URL as BU

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('vacancy_parser.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация
DATABASE_NAME = BU
REQUEST_DELAY = random.randint(3, 6)  # Задержка между запросами в секундах
MAX_RETRIES = 3  # Максимальное количество попыток повтора
BASE_URL = 'https://ukrcrewing.com.ua'


class VacancyParser:
    def __init__(self):
        self.conn = self.init_db()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def init_db(self) -> sqlite3.Connection:
        """Инициализация базы данных с новой структурой таблицы companies"""
        logger.info("Инициализация базы данных...")
        conn = sqlite3.connect("crewing.db")
        conn.row_factory = sqlite3.Row

        with conn:
            # Новая структура таблицы companies
            conn.execute('''
                CREATE TABLE IF NOT EXISTS companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    country TEXT,
                    city TEXT,
                    url TEXT,
                    phones TEXT,
                    email TEXT,
                    website TEXT,
                    address TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(name, url)
                )
            ''')

            # Таблица вакансий
            conn.execute('''
                CREATE TABLE IF NOT EXISTS vacancies (
                    id INTEGER PRIMARY KEY,
                    title TEXT,
                    published TEXT,
                    join_date TEXT,
                    contract_length TEXT,
                    vessel_type TEXT, 
                    built_year TEXT,
                    dwt TEXT,
                    engine_power TEXT,
                    crew TEXT,
                    english_level TEXT,
                    salary TEXT,
                    phone TEXT,
                    manager TEXT,
                    additional_info TEXT,
                    experience TEXT,
                    agency TEXT,
                    company_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE SET NULL
                )
            ''')

            # Таблица для хранения логов поиска компаний
            conn.execute('''
                CREATE TABLE IF NOT EXISTS company_search_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_name TEXT,
                    found BOOLEAN,
                    search_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Индексы для ускорения поиска
            conn.executescript('''
                CREATE INDEX IF NOT EXISTS idx_vacancies_company_id ON vacancies(company_id);
                CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(name);
                CREATE INDEX IF NOT EXISTS idx_vacancies_created ON vacancies(created_at);
                CREATE INDEX IF NOT EXISTS idx_company_search ON company_search_log(company_name);
            ''')

        logger.info("База данных успешно инициализирована")
        return conn

    def log_company_search(self, company_name: str, found: bool):
        """Логируем результаты поиска компании"""
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO company_search_log (company_name, found) VALUES (?, ?)",
                    (company_name, found)
                )
        except sqlite3.Error as e:
            logger.error(f"Ошибка при логировании поиска компании: {str(e)}")

    def parse_company_details(self, company_name: str) -> Optional[dict]:
        """Парсим детальную информацию о компании"""
        if not company_name:
            return None

        try:
            # Кодируем название компании для URL
            encoded_name = quote(company_name)
            search_url = f"{BASE_URL}/search?query={encoded_name}"

            logger.info(f"Поиск компании: {company_name} по URL: {search_url}")
            response = self.session.get(search_url)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Ищем ссылку на страницу компании
            company_link = None
            for a in soup.find_all('a', href=re.compile(r'/company/\d+')):
                if company_name.lower() in a.get_text().lower():
                    company_link = a
                    break

            if not company_link:
                logger.warning(f"Компания {company_name} не найдена в результатах поиска")
                self.log_company_search(company_name, False)
                return None

            company_url = company_link['href']
            full_company_url = f"{BASE_URL}{company_url}"
            logger.info(f"Найдена компания: {company_name}, URL: {full_company_url}")

            # Парсим страницу компании
            company_response = self.session.get(full_company_url)
            company_response.raise_for_status()

            company_soup = BeautifulSoup(company_response.text, 'html.parser')
            company_block = company_soup.find('div', class_='company-full-content')

            if not company_block:
                logger.warning(f"Не удалось найти блок с информацией о компании {company_name}")
                return None

            company_data = {
                'name': company_name,
                'url': full_company_url
            }

            # Парсим основные данные компании
            fields_mapping = {
                "Country:": "country",
                "City:": "city",
                "Phone:": "phones",
                "E-mail:": "email",
                "Website:": "website",
                "Address:": "address"
            }

            for row in company_block.find_all('div', class_='row'):
                cols = row.find_all('div', class_='colmn')
                if len(cols) != 2:
                    continue

                key = cols[0].get_text(strip=True)
                value = cols[1].get_text(strip=True)

                if key in fields_mapping:
                    company_data[fields_mapping[key]] = value

            # Логируем успешный поиск
            self.log_company_search(company_name, True)

            return company_data

        except requests.RequestException as e:
            logger.error(f"Ошибка запроса при поиске компании {company_name}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Неожиданная ошибка при парсинге компании {company_name}: {str(e)}")
            return None

    def get_or_create_company(self, company_name: Optional[str]) -> Optional[int]:
        """Получаем или создаем компанию и возвращаем её ID"""
        if not company_name:
            return None

        try:
            normalized_name = ' '.join(company_name.strip().split()).lower()

            with self.conn:
                cursor = self.conn.cursor()

                # Пробуем найти точное совпадение
                cursor.execute(
                    "SELECT id FROM companies WHERE LOWER(name) = ?",
                    (normalized_name,)
                )
                result = cursor.fetchone()

                if result:
                    logger.debug(f"Найдена существующая компания: {company_name} (ID: {result[0]})")
                    return result[0]

                # Пробуем найти частичное совпадение
                cursor.execute(
                    "SELECT id FROM companies WHERE LOWER(name) LIKE ? LIMIT 1",
                    (f"%{normalized_name}%",)
                )
                result = cursor.fetchone()

                if result:
                    logger.debug(f"Найдена похожая компания: {company_name} (ID: {result[0]})")
                    return result[0]

                # Парсим детальную информацию о компании
                company_data = self.parse_company_details(company_name)

                # Если не нашли компанию на сайте, создаем запись только с именем
                if not company_data:
                    cursor.execute(
                        "INSERT INTO companies (name) VALUES (?)",
                        (company_name.capitalize(),)
                    )
                else:
                    # Создаем полную запись о компании
                    cursor.execute(
                        """INSERT INTO companies (
                            name, country, city, url, phones, 
                            email, website, address
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            company_data['name'].capitalize(),
                            company_data.get('country'),
                            company_data.get('city'),
                            company_data.get('url'),
                            company_data.get('phones'),
                            company_data.get('email'),
                            company_data.get('website'),
                            company_data.get('address'),
                        )
                    )

                company_id = cursor.lastrowid
                logger.info(f"Создана новая компания: {company_name} (ID: {company_id})")
                return company_id

        except sqlite3.Error as e:
            logger.error(f"Ошибка базы данных при работе с компанией {company_name}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Неожиданная ошибка при работе с компанией {company_name}: {str(e)}")
            return None

    def update_company_info(self, company_id: int, **kwargs):
        """Обновляем информацию о компании"""
        try:
            with self.conn:
                cursor = self.conn.cursor()
                set_clause = ", ".join([f"{k} = ?" for k in kwargs.keys()])
                values = list(kwargs.values())
                values.append(company_id)

                cursor.execute(
                    f"UPDATE companies SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    values
                )
                logger.info(f"Обновлена информация о компании ID {company_id}")
                return True
        except sqlite3.Error as e:
            logger.error(f"Ошибка при обновлении компании {company_id}: {str(e)}")
            return False

    def fetch_vacancy(self, vacancy_id: int) -> Optional[dict]:
        """Получаем данные вакансии с сайта"""
        url = f'{BASE_URL}/vacancy/{vacancy_id}'
        logger.info(f"Запрос вакансии ID: {vacancy_id}")

        try:
            response = self.session.get(url)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            block = soup.find('div', class_='vacancy-full-content')

            if not block:
                logger.warning(f"Вакансия {vacancy_id} не найдена или пуста")
                return None

            data = {'id': vacancy_id}

            # Парсинг заголовка
            title = block.find('h1')
            data['title'] = title.text.strip() if title else None

            # Парсинг дат публикации и просмотров
            dates = block.find_all('div', class_='datepub')
            if dates:
                data['published'] = dates[0].get_text(strip=True).replace('Published:', '').strip() if len(
                    dates) > 0 else None
                data['views'] = dates[1].get_text(strip=True).replace('Views:', '').strip() if len(dates) > 1 else None

            # Парсинг основной информации
            fields_mapping = {
                "Joining date:": "join_date",
                "Voyage duration:": "contract_length",
                "Vessel type:": "vessel_type",
                "Year of vessel's construction:": "built_year",
                "DWT:": "dwt",
                "BHP:": "engine_power",
                "Crew on board:": "crew",
                "English level:": "english_level",
                "Salary:": "salary",
                "Employer:": "agency",
                "Phone:": "phone",
                "Manager:": "manager",
                "Additional information:": "additional_info",
                "Experience in rank:": "experience"
            }

            # Ищем все строки с информацией
            rows = block.find_all('div', class_='row')
            for row in rows:
                cols = row.find_all('div', class_='colmn')
                if len(cols) == 2:
                    key = cols[0].get_text(strip=True)
                    value = cols[1].get_text(strip=True)

                    if key in fields_mapping:
                        # Особые обработки для некоторых полей
                        if key == "Salary:":
                            salary = cols[1].find('strong')
                            value = salary.get_text(strip=True) if salary else value
                        elif key == "Employer:":
                            agency = cols[1].find('a')
                            value = agency.get_text(strip=True) if agency else value

                        data[fields_mapping[key]] = value

            # Дополнительная проверка для телефона (иногда он в отдельном блоке)
            if 'phone' not in data:
                phone_block = block.find('div', class_='phone')
                if phone_block:
                    data['phone'] = phone_block.get_text(strip=True)

            logger.debug(f"Получены данные вакансии {vacancy_id}: {data}")
            return data

        except requests.RequestException as e:
            logger.error(f"Ошибка запроса для вакансии {vacancy_id}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Неожиданная ошибка при парсинге вакансии {vacancy_id}: {str(e)}")
            return None

    def save_vacancy(self, data: dict) -> bool:
        """Сохраняем вакансию в базу данных"""
        if not data:
            return False

        try:
            # Получаем ID компании
            company_id = self.get_or_create_company(data.get('agency'))

            with self.conn:
                cursor = self.conn.cursor()

                # Вставляем или обновляем вакансию
                cursor.execute('''
                    INSERT OR REPLACE INTO vacancies(
                        id, title, published, join_date, contract_length, vessel_type, 
                        built_year, dwt, engine_power, crew, english_level, salary,
                        phone, manager, additional_info, experience, agency, company_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    data['id'],
                    data.get('title'),
                    data.get('published'),
                    data.get('join_date'),
                    data.get('contract_length'),
                    data.get('vessel_type'),
                    data.get('built_year'),
                    data.get('dwt'),
                    data.get('engine_power'),
                    data.get('crew'),
                    data.get('english_level'),
                    data.get('salary'),
                    data.get('phone'),
                    data.get('manager'),
                    data.get('additional_info'),
                    data.get('experience'),
                    data.get('agency'),
                    company_id
                ))

                logger.info(f"Вакансия {data['id']} успешно сохранена")
                return True

        except sqlite3.Error as e:
            logger.error(f"Ошибка сохранения вакансии {data.get('id', 'unknown')}: {str(e)}")
            return False

    def get_last_vacancy_id(self) -> int:
        """Получаем ID последней сохраненной вакансии"""
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute("SELECT MAX(id) FROM vacancies")
                result = cursor.fetchone()
                return result[0] if result and result[0] else 313620  # Стартовый ID
        except sqlite3.Error as e:
            logger.error(f"Ошибка получения последнего ID вакансии: {str(e)}")
            return 313620

    def process_vacancy(self, vacancy_id: int, retry_count: int = 0) -> bool:
        """Обрабатываем вакансию с повторами при ошибках"""
        try:
            data = self.fetch_vacancy(vacancy_id)
            if not data:
                if retry_count < MAX_RETRIES:
                    logger.warning(f"Повторная попытка {retry_count + 1} для вакансии {vacancy_id}")
                    sleep(REQUEST_DELAY * (retry_count + 1))
                    return self.process_vacancy(vacancy_id, retry_count + 1)
                else:
                    logger.warning(f"Вакансия {vacancy_id} не найдена после {MAX_RETRIES} попыток")
                    return False

            success = self.save_vacancy(data)
            if not success and retry_count < MAX_RETRIES:
                logger.warning(f"Повторная попытка сохранения {retry_count + 1} для вакансии {vacancy_id}")
                sleep(REQUEST_DELAY * (retry_count + 1))
                return self.process_vacancy(vacancy_id, retry_count + 1)

            return success

        except Exception as e:
            logger.error(f"Критическая ошибка обработки вакансии {vacancy_id}: {str(e)}")
            return False

    def run(self, start_id: Optional[int] = None, end_id: Optional[int] = None):
        """Основной цикл парсинга"""
        try:
            current_id = start_id if start_id else self.get_last_vacancy_id()
            end_id = end_id if end_id else current_id + 100  # По умолчанию парсим 100 вакансий

            logger.info(f"Начало парсинга с ID {current_id + 1} до {end_id}")

            for vacancy_id in range(current_id + 1, end_id + 1):
                success = self.process_vacancy(vacancy_id)
                sleep(REQUEST_DELAY)

                if not success:
                    logger.warning(f"Пропуск вакансии {vacancy_id} из-за ошибок")
                    continue

            logger.info("Парсинг успешно завершен")

        except KeyboardInterrupt:
            logger.info("Парсинг прерван пользователем")
        except Exception as e:
            logger.error(f"Критическая ошибка в основном цикле: {str(e)}")
        finally:
            self.conn.close()
            self.session.close()
            logger.info("Ресурсы освобождены")


if __name__ == "__main__":
    parser = VacancyParser()
    parser.run()