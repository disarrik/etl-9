import pyspark.sql.functions as F

sql = SQLContext(sc)
df = spark.read.option("multiline", "true").json('s3a://disarra-dz/in.json', multiLine=True)

explode_catalogs = df.select(F.explode('catalogs'))
catalogs = explode_catalogs.select('col.*')
catalogs.write.parquet('s3a://disarra-dz/catalogs.parquet')

explode_offers = df.select(F.explode('offers'))
offers = explode_offers.select('col.*')
offers.write.parquet("s3a://disarra-dz/offers.parquet")
