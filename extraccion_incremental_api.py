from funciones import get_data, build_table, save_new_data_as_delta, get_data_paginacion
from dotenv import load_dotenv
import os

load_dotenv()

api_key = os.getenv("API_KEY")
base_url = os.getenv("BASE_URL")
incremental_endpoint = os.getenv("INCREMENTAL_ENDPOINT")

headers = {
    'Authorization': f'Bearer {api_key}'
}

params = {
    'limit': '1',
}

short_volume = get_data_paginacion(base_url,incremental_endpoint,data_field='results',params=params,headers=headers)
short_volume_df = build_table(short_volume)
print(short_volume_df)
save_new_data_as_delta(short_volume_df, 'data_lake/short_volume')

