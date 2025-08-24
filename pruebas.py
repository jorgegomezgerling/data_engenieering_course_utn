# Breve script para corroborar qué hay en la carpeta deltalake y subcarpetas tickers y short_volume.
# por cuestiones de simplicidad y tratarse de un archivo de pruebas, tienen el mismo nombre las delta_table de ambos archivos
# (dt). Comento y descomento de acuerdo a lo que quiero observar.

from deltalake import DeltaTable
# dt = DeltaTable("data_lake/bronze/tickers")
dt = DeltaTable("data_lake/bronze/short_volume")
df = dt.to_pandas()
print(df.shape)
print('*********')
print(df)
