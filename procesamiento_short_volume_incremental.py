# Primeramente, cambié el parámetro en la extraccion incremental a 2. 
# De esta manera, para el mismo ticket traigo las cotizaciones de los dos ultimos dias
# Considero que puede ser util para poder hacer la limpieza de datos y quedarme con el del último día

import pandas as pd
from funciones import save_new_data_as_delta
from deltalake import DeltaTable

dt = DeltaTable("data_lake/bronze/polygon_api/short_volume")
df = dt.to_pandas()
pd.set_option("display.max_columns", None)

print(df.info(memory_usage='deep'))
# print(df)

# Estado de la cuestión antes de limpiar los datos:

# dtypes: float64(1), int64(12), object(2)
# memory usage: 4.3 KB

# Primeramente, analizamos (SI HAY) los nulos:

# print(df.isnull().sum())

# Primera transformación: castear a tipo date la fecha. 

df['date'] = pd.to_datetime(df["date"], errors="coerce")

# print(df)

# Segunda transformación quedarme con los datos de los tickets de la última fecha. 

print(df["ticker"].duplicated().sum())

# Hay diez tickers repetidos, entonces: ordenamos y nos quedamos con los más recientes.

df = df.sort_values(by=["ticker", "date"], ascending=[True, False])
df = df.drop_duplicates(subset=["ticker"], keep="first")

# Tercera Transformacion: se agrega columna que puede resultar de utilidad para posteriores análisis:

df["long_volume"] = df["total_volume"] - df["short_volume"]

# Cuarta transformación: validar ratio
df["short_volume_ratio_calc"] = (df["short_volume"] / df["total_volume"]) * 100

# Diferencia respecto al ratio original (si la hay)
df["short_ratio_diff"] = df["short_volume_ratio"] - df["short_volume_ratio_calc"]

# Quinta transformación: reorganizar los datos, para traer los datos de manera más óptima como primeros valores.

columnas = [
    "ticker",
    "date",
    "short_volume",
    "total_volume",
    "long_volume",
    "short_volume_ratio",
    "short_volume_ratio_calc",
    "short_ratio_diff",
    "adf_short_volume",
    "adf_short_volume_exempt",
    "exempt_volume",
    "nasdaq_carteret_short_volume",
    "nasdaq_carteret_short_volume_exempt",
    "nasdaq_chicago_short_volume",
    "nasdaq_chicago_short_volume_exempt",
    "non_exempt_volume",
    "nyse_short_volume",
    "nyse_short_volume_exempt"
]

df = df[columnas]


print(df)
print("************")
print(df.info(memory_usage='deep'))

# Nuevos valores posterior a limpieza, transformación y creación de nuevas tablas.

# dtypes: datetime64[ns](1), float64(3), int64(13), object(1)
# memory usage: 1.9 

save_new_data_as_delta(df, 'data_lake/silver/polygon_api/short_volume', 'total_volume')
print('guardado!')





