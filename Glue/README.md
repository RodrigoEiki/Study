# Glue

# DATA SOURCE

### Read from data catalog

```
x = glueContext.create_dynamic_frame.from_catalog(
    database="study",
    table_name="kopart",
    transformation_ctx="x",
)
```
- **Mostra o Schema do seu DF**

> x.printSchema()

- **Mostra 10 linhas**

> x.show(10)

- **Número de linhas**

> x.count()

- **Escolha a colunas que quer trabalhar**

> dfFiltrado = x.select_fields(['x', 'y', 'z'])

- **Exclui colunas**

> dfFiltrado = x.drop_fields(['x', 'y'])

- **Renomear colunas**
```
mapping = [("customerid", "long", "customerid", "long"), ("fullname", "string", "name", "string")]

dyfMapping = ApplyMapping.apply(
    frame = dyfSelectCustomers,
    mappings = mapping,
    transformation_ctx = "applymapping1"
)
```

- **Filtrar colunas**
```
dyfFilter = Filter.apply(
    frame = dyf,
    f = lambda x: x["lastname"] in "Adams"
)
```

- **Join 2 Dynamic Frames**

```
dyfJoin = dyfCustomer.join(['customerid'],['customerid'], dyfOrders)
```

- **Write Dynamic Frame to S3**
```
glueContext.write_dynamic_frame.from_options(
    frame = dyfCustomer,
    connection_type= "s3",
    connection_options={
        "path": "s3://pyspark-tutorial-digx/write_down_dyf_to_s3/"
    },
    format = "csv",
    format_options={
        "separator": ","
    },
    transformation_ctx = "datasink2"
)
```

- **Write Dynamic Frame to Data Catalog**
```
glueContext.write_dynamic_frame.from_catalog(
    frame = dyfCustomer,
    database = 'pyspark_tutorial_db',
    table_name = 'customers_write_dyf'
)
```