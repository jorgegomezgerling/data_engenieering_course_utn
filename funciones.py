import requests, pandas as pd, time
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


# Dada la cuestión de que hay paginación dentro del endpoint de short_volume debo modificar un poco get_data

def get_data_paginacion(base_url, endpoint, data_field=None, params=None, headers=None):
    all_data = []
    current_url = f'{base_url}{endpoint}'
    campos_requeridos = ['ticker', 'date', 'total_volume', 'short_volume', 'nasdaq_carteret_short_volume']
    contador = 0

    while current_url:
        try:
            if current_url == f'{base_url}{endpoint}':
                response = requests.get(current_url, params=params, headers=headers)
            else:
                response = requests.get(current_url, headers=headers)
            # print(response)
            
            if response.status_code == 429:
                print("Rate limit excedido. Esperando 60 segundos antes de continuar.")
                time.sleep(60)
                continue

            response.raise_for_status()

            try:
                data = response.json()
            except Exception as e:
                print("Error al convertir a json.")
                break;
            
            if data_field:
                page_data = data.get(data_field, [])
                for item in page_data:
                    item_filtrado = {field: item.get(field) for field in campos_requeridos}
                    all_data.append(item_filtrado)
            else:
                page_data = data
                all_data.extend(page_data)
            
            

            current_url = data.get('next_url')
            contador += 1

            if contador == 5:
                break;
            
            time.sleep(5)

        except Exception as e:
            print(f'Error en carga de datos inicial {e}')
            break;
    
    return all_data


    
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

def save_new_data_as_delta(new_data, data_path, partition_cols=None, predicate=None):
    if predicate is None:
        predicate = "target.ticker = source.ticker AND target.date = source.date"
    try:
        dt = DeltaTable(data_path)
        new_data_pa = pa.Table.from_pandas(new_data)
        dt.merge(
            source=new_data_pa,
            source_alias='source',
            target_alias='target',
            predicate=predicate
        ) \
        .when_not_matched_insert_all() \
        .execute()
    
    except TableNotFoundError:
        save_data_as_delta(new_data, data_path, partition_cols=partition_cols)
