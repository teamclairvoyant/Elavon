from dq_rule_execution_engine.src.utils import get_spark_session


def parquet(entity, query):
    path = [entity_property for entity_property in entity['properties']
            if entity_property['key'] == 'PATH'][0]['value']
    data = get_spark_session().read.parquet(path, header=True)
    data.registerTempTable(entity['entity_physical_name'])
    return get_spark_session().sql(query)


def csv(entity, query):
    path = [entity_property for entity_property in entity['properties']
            if entity_property['key'] == 'PATH'][0]['value']
    data = get_spark_session() \
        .read \
        .csv(path, header=True, inferSchema=True) \
        .na.drop("all")  # To drop records with null values in all the columns.
    data.registerTempTable(entity['entity_physical_name'])
    return get_spark_session().sql(query)


def delta(entity, query):
    path = [entity_property for entity_property in entity['properties']
            if entity_property['key'] == 'PATH'][0]['value']
    get_spark_session() \
        .read.format("delta") \
        .load(path) \
        .na.drop("all").createOrReplaceTempView("delta_table")  # To drop records with null values in all the columns.
    data = get_spark_session().sql("""
    SELECT *
    FROM delta_table
    WHERE date_partition = (
        SELECT MAX(date_partition)
        FROM delta_table
    )""")
    data = data.drop(data['date_partition'])
    data.registerTempTable(entity['entity_physical_name'])
    return get_spark_session().sql(query)


def sql_server(entity, query, context):
    db_url = 'jdbc:sqlserver://{host}:{port};database={dbname}'.format(
        host=context.get_value('sql_host'),
        port=context.get_value('sql_port'),
        dbname=context.get_value('sql_dbname')
    )
    properties = {
        'user': context.get_value('sql_user'),
        'password': context.get_value('sql_password'),
        'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
    }
    data = get_spark_session().read.jdbc(url=db_url, table=entity['entity_physical_name'], properties=properties)
    data.createOrReplaceTempView(entity['entity_name'])
    return get_spark_session().sql(query)


def snowflake(entity, query, context):
    sfURL = context.get_value('snowflake_account_url')
    sfUser = context.get_value('snowflake_user')
    sfDatabase = context.get_value('snowflake_database')
    sfPassword = context.get_value('snowflake_password')
    sfSchema = context.get_value('snowflake_schema')
    sfWarehouse = context.get_value('snowflake_warehouse')
    sfOptions = {
        "sfURL": sfURL,
        "sfUser": sfUser,
        "sfDatabase": sfDatabase,
        "sfPassword": sfPassword,
        "sfWarehouse": sfWarehouse,
        "sfSchema": sfSchema
    }
    data = get_spark_session().read \
        .format("snowflake") \
        .options(**sfOptions) \
        .option("dbtable", entity['entity_physical_name']) \
        .load()
    data.createOrReplaceTempView(entity['entity_name'])
    return get_spark_session().sql(query)


def api_data(entity, query, context):
    api_url = context.get_value('api_url')
    params = json.loads(context.get_value('params'))
    headers = context.get_value('headers')
    arr = context.get_value('arr')
    if headers:
        response = requests.get(api_url, params=params, headers=headers)
    else:
        response = requests.get(api_url, params=params)
    if response.status_code == 200:
        api_data = response.json()
        if arr:
            api_data = api_data[str(arr)]
        data = get_spark_session().createDataFrame(api_data)
    data.registerTempTable(entity['entity_name'])
    return get_spark_session().sql(query)


def mongodb(entity, query, context):
    mongodb_uri = context.get_value('mongodb_uri')
    mongodb_database = context.get_value('mongodb_database')
    mongodb_collection = context.get_value('mongodb_collection')
    data = get_spark_session().read.format("mongo") \
        .option("uri", mongodb_uri) \
        .option("database", mongodb_database) \
        .option("collection", mongodb_collection) \
        .load()
    data.registerTempTable(entity['entity_name'])
    return get_spark_session().sql(query)


def big_query(entity, query, context):
    get_spark_session().conf.set('temporaryGcsBucket', context.get_value('temp_gcs_bucket_name'))
    get_spark_session().conf.set('materializationDataset', context.get_value('bq_dataset'))

    data = get_spark_session().read.format('bigquery'). \
        option('project', context.get_value('project_id')). \
        option('table', entity['entity_physical_name']). \
        load()

    data.registerTempTable(entity['entity_name'])
    return get_spark_session().sql(query)


def hive(query):
    return get_spark_session().sql(query)


def read(entity, query, context):
    entity_sub_type = entity['entity_sub_type']
    data = None
    if entity_sub_type == 'CSV':
        data = csv(entity, query)
    if entity_sub_type == 'PARQUET':
        data = parquet(entity, query)
    if entity_sub_type == 'BIG_QUERY':
        data = big_query(entity, query, context)
    if entity_sub_type == 'HIVE':
        data = hive(query)
    if entity_sub_type == 'SQL':
        data = sql_server(entity, query, context)
    if entity_sub_type == 'SNOWFLAKE':
        data = snowflake(entity, query, context)
    if entity_sub_type == 'API':
        data = api_data(entity, query, context)
    if entity_sub_type == 'MONGODB':
        data = mongodb(entity, query, context)
    if entity_sub_type == 'DELTA':
        data = delta(entity, query)
    return data
