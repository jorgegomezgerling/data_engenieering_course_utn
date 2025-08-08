from funciones import get_data, build_table, save_data_as_delta
from dotenv import load_dotenv
import os

load_dotenv()

api_key = os.getenv("API_KEY")
base_url = os.getenv("BASE_URL")
incremental_endpoint = os.getenv("INCREMENTAL_ENDPOINT")

headers = {
    'Authorization': f'Bearer {api_key}'
}

short_volume = get_data(base_url,incremental_endpoint,data_field='results',headers=headers)
print(short_volume)
# short_volume_df = build_table(short_volume)
# save_data_as_delta(tickers_df, "data_lake/tickers", mode='overwrite')
# print(short_volume_df.tail())