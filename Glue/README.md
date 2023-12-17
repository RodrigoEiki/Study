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