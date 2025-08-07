from deltalake import DeltaTable

dt = DeltaTable("data_lake/tickers")

df = dt.to_pandas()
print(df.head())
print(df.shape)