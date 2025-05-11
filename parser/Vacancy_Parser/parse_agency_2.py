from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import sqlite3
from time import sleep
import logging

from config import BASE_URL_AGENCY, DATABASE_NAME

# Настройки
BASE_URL = BASE_URL_AGENCY
DB_NAME = DATABASE_NAME  # Используем ту же базу данных, что и для вакансий
MAX_PAGES = 51
REQUEST_DELAY = 2  # Задержка между запросами в секундах

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class CrewingParser:
    def __init__(self):
        self.setup_driver()
        self.setup_database()

    def setup_driver(self):
        """Инициализация веб-драйвера"""
        logger.info("Инициализация веб-драйвера...")
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        self.driver = webdriver.Chrome(options=chrome_options)
        logger.info("Веб-драйвер инициализирован.")

    def setup_database(self):
        """Инициализация базы данных"""
        logger.info("Инициализация базы данных...")
        self.conn = sqlite3.connect(DB_NAME)
        self.cursor = self.conn.cursor()
        self.create_tables()
        logger.info("База данных инициализирована.")

    def create_tables(self):
        """Создание таблицы companies в существующей базе данных"""
        logger.info("Создание таблицы companies...")

        # Проверяем, существует ли уже таблица companies
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='companies'")
        if not self.cursor.fetchone():
            self.cursor.execute('''
                CREATE TABLE companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    country TEXT,
                    city TEXT,
                    url TEXT,
                    phones TEXT,
                    email TEXT,
                    website TEXT,
                    address TEXT,
                    UNIQUE(name, url)
                )
            ''')
            self.conn.commit()
            logger.info("Таблица companies создана.")
        else:
            logger.info("Таблица companies уже существует.")

    def scrape_main_pages(self):
        """Парсинг списка компаний с основных страниц"""
        logger.info("Начало парсинга основных страниц")

        for page in range(1, MAX_PAGES + 1):
            logger.info(f"Парсинг страницы {page}...")
            url = BASE_URL.format(page)

            try:
                self.driver.get(url)
                sleep(REQUEST_DELAY)
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                table = soup.find('tbody')

                if not table:
                    logger.info(f"На странице {page} нет данных. Завершение.")
                    break

                companies = []
                for row in table.find_all('tr'):
                    cols = row.find_all('td')
                    if len(cols) >= 3:
                        name_link = cols[0].find('a')
                        name = name_link.get_text(strip=True) if name_link else cols[0].get_text(strip=True)
                        link = "https://ukrcrewing.com.ua" + name_link['href'] if name_link and name_link.get(
                            'href') else None
                        country = cols[1].get_text(strip=True)
                        city = cols[2].get_text(strip=True)

                        companies.append({
                            'name': name,
                            'country': country,
                            'city': city,
                            'url': link
                        })

                self.save_companies(companies)
                logger.info(f"Добавлено {len(companies)} компаний на страницу {page}.")
                sleep(REQUEST_DELAY)

            except Exception as e:
                logger.error(f"Ошибка при парсинге страницы {page}: {str(e)}")
                continue

    def scrape_company_details(self):
        """Парсинг детальной информации о компаниях"""
        logger.info("Начало парсинга детальной информации")

        self.cursor.execute("SELECT id, url FROM companies WHERE url IS NOT NULL AND phones IS NULL")
        companies = self.cursor.fetchall()
        total = len(companies)

        if not total:
            logger.info("Нет компаний для обновления информации")
            return

        logger.info(f"Найдено компаний для обновления: {total}")

        for idx, (company_id, url) in enumerate(companies, 1):
            logger.info(f"Обработка компании {idx}/{total}: {url}")

            try:
                self.driver.get(url)
                sleep(REQUEST_DELAY)
                soup = BeautifulSoup(self.driver.page_source, "html.parser")

                data = {
                    "phones": self.extract_detail(soup, "Phone number:"),
                    "email": self.extract_detail(soup, "E-mail:"),
                    "website": self.extract_detail(soup, "Website:"),
                    "address": self.extract_detail(soup, "Address:")
                }

                logger.info(f"→ Получены данные для компании ID {company_id}: {data}")
                self.update_company(company_id, data)
                sleep(REQUEST_DELAY)

            except Exception as e:
                logger.error(f"Ошибка при обработке компании {url}: {str(e)}")
                continue

    @staticmethod
    def extract_detail(soup, label):
        """Извлечение данных с детальной страницы"""
        # Обработка телефонных номеров
        if "phone" in label.lower():
            phones = []
            # Находим блок с меткой "Phone number:"
            phone_label = soup.find('span', string=lambda t: t and "phone number:" in str(t).lower())

            if phone_label:
                # Получаем родительский div
                phone_div = phone_label.find_parent('div', class_='colmn')
                if phone_div:
                    # Извлекаем первый номер (после метки)
                    first_phone = phone_div.get_text(strip=True).replace(phone_label.get_text(strip=True), "").strip()
                    if first_phone:
                        phones.append(first_phone)

                    # Ищем следующие div.colmn без span (дополнительные номера)
                    next_div = phone_div.find_next_sibling('div', class_='colmn')
                    while next_div and not next_div.find('span'):
                        phone_text = next_div.get_text(strip=True)
                        if phone_text:
                            phones.append(phone_text)
                        next_div = next_div.find_next_sibling('div', class_='colmn')

            return ", ".join(phones) if phones else None

        # Для других данных (email, website, address)
        for div in soup.select("div.colmn"):
            span = div.find("span")
            if span and label.lower() in span.get_text(strip=True).lower():
                text = div.get_text(strip=True).replace(span.get_text(strip=True), "").strip()
                # Для email и website извлекаем ссылку, если есть
                if "mail" in label.lower() or "website" in label.lower():
                    link = div.find('a')
                    if link:
                        return link.get('href', '').replace('mailto:', '').strip()
                return text

        return None

    def save_companies(self, companies):
        """Сохранение компаний в базу данных"""
        logger.info("Сохранение компаний в базу данных...")
        try:
            for company in companies:
                self.cursor.execute('''
                    INSERT OR IGNORE INTO companies (name, country, city, url)
                    VALUES (?, ?, ?, ?)
                ''', (company['name'], company['country'], company['city'], company['url']))
            self.conn.commit()
            logger.info(f"Успешно сохранено {len(companies)} компаний.")
        except sqlite3.Error as e:
            logger.error(f"Ошибка при сохранении данных: {str(e)}")

    def update_company(self, company_id, data):
        """Обновление информации о компании"""
        try:
            self.cursor.execute('''
                UPDATE companies SET phones = ?, email = ?, website = ?, address = ?
                WHERE id = ?
            ''', (data["phones"], data["email"], data["website"], data["address"], company_id))
            self.conn.commit()
            logger.info(f"Компания ID {company_id} успешно обновлена.")
        except sqlite3.Error as e:
            logger.error(f"Ошибка при обновлении компании {company_id}: {str(e)}")

    def close(self):
        """Закрытие соединений"""
        self.driver.quit()
        self.conn.close()
        logger.info("Ресурсы освобождены")


def main():
    parser = CrewingParser()
    try:
        # parser.scrape_main_pages()
        parser.scrape_company_details()
    except KeyboardInterrupt:
        logger.info("Парсинг прерван пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}")
    finally:
        parser.close()
        logger.info("Работа завершена")


if __name__ == "__main__":
    main()