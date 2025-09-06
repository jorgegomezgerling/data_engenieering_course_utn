# Extraccion full: importadas las funciones necesarias y manejo de claves a traves de un archivo .env
# y lectura a traves del load_dotenv
# Se utiliza el modo overwrite porque estamos hablando de una extracción full y lo consideré lo más pertinente.

from funciones import get_data, build_table, save_data_as_delta
from dotenv import load_dotenv
import os

load_dotenv()

api_key = os.getenv("API_KEY")
base_url = os.getenv("BASE_URL")
full_endpoint = os.getenv("FULL_ENDPOINT")

headers = {
    'Authorization': f'Bearer {api_key}'
}

tickers = get_data(base_url,full_endpoint, data_field='results',headers=headers)
tickers_df = build_table(tickers)
save_data_as_delta(tickers_df, "data_lake/bronze/polygon_api/tickers", mode='overwrite')
print(tickers_df)