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
save_data_as_delta(tickers_df, "data_lake/tickers", mode='overwrite')