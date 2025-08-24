import pandas as pd
from funciones import save_data_as_delta
from deltalake import DeltaTable

dt = DeltaTable("data_lake/bronze/tickers")
df = dt.to_pandas()
# pd.set_option("display.max_columns", None)

print(df.info(memory_usage='deep'))
print(df)


# Sin limpieza: dtypes: bool(1), object(11) memory usage: 59.6 KB

# Primeramente, analizamos los nulos:

print(df.isnull().sum())

# Primera limpieza: Dado que la columna 'cik' unicamente tiene 36 campos con datos de los 100, considero oportuno eliminarla.

df = df.drop(columns=["cik"])

# Segunda limpieza: los campos 'composite_figi', 'share_class_figi' tienen 31 y 32 campos vacios respectivamente,
# por lo cual voy a imputarlos. 

df['composite_figi'] = df["composite_figi"].fillna('UNKNOWN')
df['share_class_figi'] = df["share_class_figi"].fillna('UNKNOWN')

# Tercera limpieza: en curreny_name tenemos tanto "usd" como "USD", conviene unificar criterio.

df["currency_name"] = df["currency_name"].str.lower()

# Cuarta limpieza: las fechas están en formato object y conviene traerlas a datime

df['last_updated_utc'] = pd.to_datetime(df["last_updated_utc"], errors="coerce")

# Quinta limpieza: Analizamos posibles valores categóricos 

print(df["market"].unique())
print(df["type"].unique())
print(df["primary_exchange"].unique())

# Categorizamos: 

cat_cols = ['market', "type", "primary_exchange"]

for col in cat_cols:
    df[col] = df[col].astype("category")


print("******* NUEVOS VALORES *******")
print(df.isnull().sum())
print(df.info(memory_usage='deep'))

# Se redujo el uso de memoria a: 
# dtypes: bool(1), category(3), datetime64[ns, UTC](1), object(6)
# memory usage: 37.5 KB

print(df)
save_data_as_delta(df, "data_lake/silver/tickers", mode='overwrite')