# Se importan todas las librerias necesarias.

import requests, pandas as pd, time
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
from datetime import datetime, timedelta

def get_data(base_url, endpoint, data_field=None, params=None, headers=None):
    """
    Realiza una petición GET con los parámetros especificados.
    base_url: url básica de la api.
    endpoint: endpoint deseado.
    data_field: campo sobre el cuál filtrar, de estar vacío se ignora.
    params: parámetros para especificar el GET.
    headers: por donde en nuestro caso pasamos las credenciales.
    """
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


# Dada la naturaleza del endpoint elegido, tengo que modificar el get_data base, 
# para de esta manera poder manejar paginación y traer diferentes tickers. 

def get_data_paginacion(base_url, endpoint, data_field=None, params=None, headers=None):
    """
    Los parametros son exactamente iguales que en su funcion base 'get_data'.
    La difrencia radica en que el url primero se forma a partir de los parámetros,
    y posteriormente se lo extrae del data_field 'next_url'. Idealmente, cuando no hay más páginas para cargar,
    el proceso se detiene, ya que next_url es una lista vacía y current_url deviene False.

    Esto es necesario debido a que la API por cada llamado al endpoint trae un gran volumen de histórico por cada TICKER.
    Es decir, traería alrededor de 50 registros únicamente del TICKER 'A'. 
    Lo cual (entiendo) actúa contra lo que se precisa en el ejercicio mismo, de manejar un histórico nosotros.
    De esta forma, con el manejo de 'next_url' me proveo de mayor variedad de tickers.

    No obstante, dado que es copioso el volumen de datos que trae, y que se debe manejar un 429 por límites propios de la API
    ingresé una variable contador para cortar el programa adrede en cierto número de llamado a la API. En este ultimo llamado,
    utilicé el número 8 como contador. Lo cual ingresa en únicamente un 429. 
    """
    all_data = []
    current_url = f'{base_url}{endpoint}'
    campos_requeridos = ['ticker', 'date', 'total_volume', 'short_volume', 'exempt_volume','short_volume_ratio', 'nasdaq_carteret_short_volume']
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

            if contador == 8:
                break;
            
            time.sleep(5)

        except Exception as e:
            print(f'Error en carga de datos inicial {e}')
            break;
    
    return all_data

    
def build_table(json_data):
    """
    Crear un dataframe a partir de un JSON a partir del método json_normalize,
    si no se pasan los datos en el formato necesario se envía una excepción.
    """
    try:
        df = pd.json_normalize(json_data)
        return df
    except:
        print("Los datos no estan en el formato esperado")
        return None

def save_data_as_delta(df, path, mode='overwrite', partition_cols=None):
    """
    Se guarda la data como un delta. 
    Los parámetros son el dataframe (df), la ruta en dónde se alojarán los archivos (path) -si no existe la crea-,
    el modo (mode): en este caso utilicé overwrite dado que utilizo esta funcion en extraccion_full y partitions_cols 
    si se desea particionar los datos de acuerdo a alguna columna en particular.
    """
    write_deltalake(
        path, df, mode=mode, partition_by=partition_cols
    )

def save_new_data_as_delta(new_data, data_path, partition_cols=None, predicate=None):
    """
    Los parámetros en esta función son los mismos que en save_data_as_delta, con la excepción de 'predicate'
    que se encuentra solo en ésta función. Este parámetro nos sirve para poder identificar qué datos insertar y qué
    registros ignorar.
    En este caso, entonces, el predicado (en caso de ser vacío por parámetros), toma como clave primaria el target.ticker y target.date, 
    y lo compara con source.ticket y source.date, si coinciden los campos no hace nada, si no coinciden los campos ingresa nuevo registro.
    Finalmente, si la tabla no se encuentra simplemente llama a la función 'save_data_as_delta' para que ingrese la nueva data de forma sencilla,
    esto es, sin filtro por predicate.
    """
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
