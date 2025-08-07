import requests, pandas as pd
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
from datetime import datetime, timedelta

def get_data(base_url, endpoint, data_field=None, params=None, headers=None):
    try:
        endpoint_url = f'{base_url}{endpoint}'
        response = requests.get(endpoint_url, params=params, headers=headers)
        print(response)
        response.raise_for_status()

        try:
            data = response.json()
            if data_field:
                data = data[data_field]
        except:
            print("El formato de respuesta no es el esperado.")
            return None
        return data
    except requests.exceptions.RequestException as e:
        print("La peticion ha fallado. Codigo de error: {e}")
        return None
    
def build_table(json_data):
    try:
        df = pd.json_normalize(json_data)
        return df
    except:
        print("Los datos no estan en el formato esperado")
        return None

def save_data_as_delta(df, path, mode='overwrite', partition_cols=None):
    write_deltalake(
        path, df, mode=mode, partition_by=partition_cols
    )

