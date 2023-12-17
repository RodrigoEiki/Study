# Spark

- **Converter para Spark DF**

```
sparkDF = dyf.toDF()
```

- **Select Columns**

selectDF = sparkDF.select("customerid", "fullname")

- **Adicionar coluna com um valor fixo**
```
from pyspark.sql.functions import lit

dfNewColumn = sparkDF.withColumn("date", lit("2022-07-25"))
```