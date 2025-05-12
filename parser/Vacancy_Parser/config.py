import random


#### TELEGRAM BOT VACANCY####

BOT_TOKEN_VACANCY = "7811660856:AAFjc4uVeEtcH6iPKhoezCns9H7z0dRa7_c"  # Replace with your actual bot token
GROUP_ID_VACANCY = "@Crewing_Agent_Vacancy"  # Replace with your group/channel ID or username
ADMIN_IDS_VACANCY = [1009961747]  # Replace with your admin user ID(s)

# #### TELEGRAM BOT RESUME####
BOT_TOKEN_RESUME = "7722001108:AAG_SscRN4uGHh94qmxV10MppozG1x78UvY"  # Replace with your actual bot token
ADMIN_IDS_RESUME = [1009961747]  # Replace with your admin user ID(s)

#### AGENCY PARSER ####
BASE_URL_AGENCY = "https://ukrcrewing.com.ua/en/agency/p{}/"
DB_NAME = 'crewing.db'  # Используем ту же базу данных, что и для вакансий
MAX_PAGES = 55
REQUEST_DELAY = 2  # Задержка между запросами в секундах

#### VACANCY PARSER ####
DATABASE_NAME = '/Users/andrii_zadorozhnii/Documents/Seaman_Vacancy/parser/Vacancy_Parser/crewing.db'
REQUEST_DELAY = random.randint(3, 6)
MAX_RETRIES = 3
BASE_URL = 'https://ukrcrewing.com.ua'


#### API KEYS ####
# https://app.sendgrid.com/settings/api_keys
SENDGRID_APIKEY = "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"
EMAIL = 'seamanresumesender@gmail.com'

