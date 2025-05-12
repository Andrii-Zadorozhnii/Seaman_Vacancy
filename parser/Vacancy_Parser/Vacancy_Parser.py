import logging
from datetime import datetime
import requests
import sqlite3
from bs4 import BeautifulSoup
from time import sleep
import random
from typing import Optional
from config import BASE_URL as BASE_URL_VACANCY, DATABASE_NAME as DB

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("vacancy_parser.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

DATABASE_NAME = DB
REQUEST_DELAY = random.randint(3, 6)
MAX_RETRIES = 3
BASE_URL = BASE_URL_VACANCY


class VacancyParser:
    def __init__(self):
        self.conn = self.init_db()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
        )

    def init_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(DATABASE_NAME)
        conn.row_factory = sqlite3.Row

        with conn:
            conn.execute(
                """
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
                    UNIQUE(name, url)
                )
            """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS vacancies (
                    id INTEGER PRIMARY KEY,
                    title TEXT,
                    published TEXT,
                    join_date TEXT,
                    contract_length TEXT,
                    vessel_type TEXT,
                    vessel_name TEXT,
                    built_year TEXT,
                    dwt TEXT,
                    engine_type TEXT,
                    engine_power TEXT,
                    crew TEXT,
                    english_level TEXT,
                    age_limit TEXT,
                    visa_required TEXT,
                    salary TEXT,
                    phone TEXT,
                    email TEXT,
                    email_subject TEXT,
                    manager TEXT,
                    additional_info TEXT,
                    experience TEXT,
                    experience_type_vessel TEXT,
                    views TEXT,
                    agency TEXT,
                    website TEXT,
                    company_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE SET NULL
                )
            """
            )

        return conn

    def get_or_create_company(self, company_name: Optional[str]) -> Optional[int]:
        """Получаем или создаем компанию и возвращаем её ID"""
        if not company_name:
            return None

        try:
            normalized_name = " ".join(company_name.strip().split()).lower()

            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute(
                    "SELECT id FROM companies WHERE LOWER(name) = ?", (normalized_name,)
                )
                result = cursor.fetchone()

                if result:
                    return result[0]

                cursor.execute(
                    "INSERT INTO companies (name) VALUES (?)",
                    (company_name.capitalize(),),
                )
                return cursor.lastrowid

        except sqlite3.Error as e:
            logger.error(f"Ошибка при работе с компанией {company_name}: {str(e)}")
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
                    f"UPDATE companies SET {set_clause} WHERE id = ?", values
                )
                return True
        except sqlite3.Error as e:
            logger.error(f"Ошибка при обновлении компании {company_id}: {str(e)}")
            return False

    def fetch_vacancy(self, vacancy_id: int) -> Optional[dict]:
        """Получаем данные вакансии с сайта"""
        url = f"{BASE_URL}/en/vacancy/{vacancy_id}"
        logger.info(f"Запрос вакансии ID: {vacancy_id}")

        normalized_mapping = {
            "Joining date": "join_date",
            "Voyage duration": "contract_length",
            "Sailing area": "sailing_area",
            "Vessel type": "vessel_type",
            "Vessel name": "vessel_name",
            "Year of vessel's construction": "built_year",
            "DWT": "dwt",
            "Main engine type": "engine_type",
            "BHP": "engine_power",
            "Crew on board": "crew",
            "English level": "english_level",
            "Age limit": "age_limit",
            "Visa available": "visa_required",
            "Experience in rank": "experience",
            "Experience on the same type vessel": "experience_type_vessel",
            "Tel № to apply for a job": "phone",
            "Contact e-mail": "email",
            "Recommended e-mail subject": "email_subject",
            "Relevant manager name": "manager",
            "Employer": "agency",
            "Website": "website",
        }

        try:
            response = self.session.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            block = soup.find("div", class_="vacancy-full-content")

            if not block:
                logger.warning(f"Вакансия {vacancy_id} не найдена (отсутствует блок)")
                return None

            data = {"id": vacancy_id}

            # Заголовок вакансии
            title_tag = block.find("h1")
            data["title"] = (
                title_tag.get_text(strip=True).replace("Vacancy", "", 1).strip()
                if title_tag
                else None
            )

            # Дата публикации и просмотры
            date_pubs = block.find_all("div", class_="datepub")
            if date_pubs:
                data["published"] = (
                    date_pubs[0].get_text(strip=True).replace("Posted:", "").strip()
                )
                if len(date_pubs) > 1:
                    data["views"] = (
                        date_pubs[1].get_text(strip=True).replace("Views:", "").strip()
                    )

            # Основные поля вакансии
            for col in block.find_all("div", class_="colmn"):
                span = col.find("span")
                if not span:
                    continue

                raw_key = span.get_text(strip=True)
                normalized_key = raw_key.replace(":", "").strip()

                if normalized_key in normalized_mapping:
                    db_field = normalized_mapping[normalized_key]

                    # Специальная обработка для разных полей
                    if normalized_key == "Employer":
                        agency = col.find("a")
                        data[db_field] = (
                            agency.get_text(strip=True)
                            if agency
                            else col.get_text().replace(raw_key, "").strip()
                        )
                    elif normalized_key == "Contact e-mail":
                        email = col.find("a")
                        data[db_field] = (
                            email.get_text(strip=True)
                            if email
                            else col.get_text().replace(raw_key, "").strip()
                        )
                    elif normalized_key == "Website":
                        website = col.find("a")
                        if website and website.has_attr("href"):
                            data[db_field] = website["href"].strip()
                        else:
                            text = col.get_text().replace(raw_key, "").strip()
                            data[db_field] = text if text and text != "—" else None
                    else:
                        data[db_field] = col.get_text().replace(raw_key, "").strip()

            # Зарплата
            salary_block = block.find("div", class_="priceBig")
            if salary_block:
                salary = salary_block.find("strong")
                if salary:
                    data["salary"] = salary.get_text(strip=True)

            # Дополнительная информация
            add_info_tag = block.find(
                "strong", class_="site_subtitle", string="Additional info:"
            )
            if add_info_tag:
                info_text = []
                next_elem = add_info_tag.next_sibling
                while next_elem and not (
                    next_elem.name == "hr" and "hr2" in next_elem.get("class", [])
                ):
                    if isinstance(next_elem, str):
                        info_text.append(next_elem.strip())
                    elif next_elem.name == "br":
                        info_text.append("\n")
                    elif hasattr(next_elem, "get_text"):
                        info_text.append(next_elem.get_text(strip=True))
                    next_elem = next_elem.next_sibling
                data["additional_info"] = " ".join(
                    [line for line in info_text if line.strip()]
                ).strip()
            logger.debug(f"Raw website data: {data.get('website')}")
            logger.debug(f"Данные вакансии {vacancy_id}: {data}")
            return data

        except Exception as e:
            logger.error(f"Ошибка при получении вакансии {vacancy_id}: {str(e)}")
            return None

    def save_vacancy(self, data: dict) -> bool:
        """Сохраняем вакансию в базу данных"""
        if not data or "id" not in data:
            logger.error("Нет данных или ID для сохранения")
            return False

        try:
            # Получаем или создаем компанию
            company_id = None
            if data.get("agency"):
                company_id = self.get_or_create_company(data["agency"])
                if company_id and data.get("website"):
                    website = data["website"].strip()
                    if website and not website.startswith(("http://", "https://")):
                        website = f"http://{website}"
                    self.update_company_info(company_id, website=website)

            # Подготовка данных для вставки (только существующие колонки)
            columns = [
                "id",
                "title",
                "published",
                "join_date",
                "contract_length",
                "vessel_type",
                "vessel_name",
                "built_year",
                "dwt",
                "engine_type",
                "engine_power",
                "crew",
                "english_level",
                "age_limit",
                "visa_required",
                "salary",
                "phone",
                "email",
                "manager",
                "additional_info",
                "experience",
                "agency",
                "website",
            ]

            # Добавляем company_id в данные, если оно есть
            if company_id is not None:
                columns.append("company_id")
                data["company_id"] = company_id

            values = [data.get(col) for col in columns]

            with self.conn:
                cursor = self.conn.cursor()
                placeholders = ",".join(["?"] * len(columns))

                # Проверяем, есть ли company_id в таблице
                cursor.execute("PRAGMA table_info(vacancies)")
                table_columns = [col[1] for col in cursor.fetchall()]

                if "company_id" in table_columns:
                    update_set = ",".join(
                        [f"{col}=excluded.{col}" for col in columns if col != "id"]
                    )
                    query = f"""
                        INSERT INTO vacancies({','.join(columns)})
                        VALUES ({placeholders})
                        ON CONFLICT(id) DO UPDATE SET {update_set}
                    """
                else:
                    # Если колонки company_id нет, сохраняем без нее
                    logger.warning("Колонка company_id отсутствует в таблице vacancies")
                    update_set = ",".join(
                        [
                            f"{col}=excluded.{col}"
                            for col in columns
                            if col != "id" and col != "company_id"
                        ]
                    )
                    query = f"""
                        INSERT INTO vacancies({','.join(col for col in columns if col != 'company_id')})
                        VALUES ({','.join(['?'] * (len(columns) - 1))})
                        ON CONFLICT(id) DO UPDATE SET {update_set}
                    """
                    values = [
                        v for v, col in zip(values, columns) if col != "company_id"
                    ]

                cursor.execute(query, values)
                logger.info(
                    f"Вакансия {data['id']} сохранена. Company ID: {company_id}"
                )
                return True

        except sqlite3.Error as e:
            logger.error(
                f"Ошибка SQL при сохранении вакансии {data.get('id')}: {str(e)}"
            )
            return False
        except Exception as e:
            logger.error(
                f"Неожиданная ошибка при сохранении {data.get('id')}: {str(e)}"
            )
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
                    logger.warning(
                        f"Повторная попытка {retry_count + 1} для вакансии {vacancy_id}"
                    )
                    sleep(REQUEST_DELAY * (retry_count + 1))
                    return self.process_vacancy(vacancy_id, retry_count + 1)
                return False

            success = self.save_vacancy(data)
            if not success and retry_count < MAX_RETRIES:
                logger.warning(
                    f"Повторная попытка сохранения {retry_count + 1} для вакансии {vacancy_id}"
                )
                sleep(REQUEST_DELAY * (retry_count + 1))
                return self.process_vacancy(vacancy_id, retry_count + 1)

            return success

        except Exception as e:
            logger.error(
                f"Критическая ошибка обработки вакансии {vacancy_id}: {str(e)}"
            )
            return False

    def run(self, start_id: Optional[int] = None, end_id: Optional[int] = None):
        """Основной цикл парсинга"""
        try:
            current_id = start_id if start_id else self.get_last_vacancy_id()
            end_id = end_id if end_id else current_id + 100

            logger.info(f"Начало парсинга с ID {current_id + 1} до {end_id}")

            for vacancy_id in range(current_id + 1, end_id + 1):
                success = self.process_vacancy(vacancy_id)
                sleep(REQUEST_DELAY)

                if not success:
                    logger.warning(f"Пропуск вакансии {vacancy_id} из-за ошибок")

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
