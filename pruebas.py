from deltalake import DeltaTable
dt = DeltaTable("data_lake/tickers")
# dt = DeltaTable("data_lake/short_volume")
df = dt.to_pandas()
print(df.shape)
print('*********')
print(df)
