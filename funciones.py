import requests

def get_data(base_url, endpoint, data_field=None, params=None, headers=None):
    try:
        endpoint_url = f'{base_url}/{endpoint}'
        response = requests.get(endpoint_url, params=params, headers=headers)
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