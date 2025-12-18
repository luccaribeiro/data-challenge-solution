import yaml

_ATHENA_CLIENT = None

TYPE_MAPPING = {
    "string": "STRING",
    "integer": "INT",
    "number": "DOUBLE",
    "boolean": "BOOLEAN",
    "object": "STRUCT",
    "array": "ARRAY"
}

def load_schema(path):
    with open(path) as f:
        schema = yaml.safe_load(f)
    return schema

def create_hive_table_with_athena(query):
    
    print(f"Query: {query}")
    return _ATHENA_CLIENT.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': f's3://iti-query-results/'
        }
    )

def handler(tablename, location):
    schema = load_schema("desafios/exercicio2/schema.json")
    columns = ""
    for column, info in schema['properties'].items():
        if info["type"] == 'object':
            struct = f'{column} STRUCT<'
            for column_object, info_object in info['properties'].items():
                struct += f"{column_object}:{TYPE_MAPPING[info_object['type']]},"
            
            struct = struct.rstrip(',') 
            
            struct += ">"
            columns += struct + ", " 
        else:
            columns += f"{column} {TYPE_MAPPING[info['type']]}, "
    
    columns = columns.rstrip(', ') 
    
    query = f"""CREATE EXTERNAL TABLE {tablename} (
       {columns}
    )
    STORED AS PARQUET
    LOCATION '{location}' """
    
    return create_hive_table_with_athena(query)