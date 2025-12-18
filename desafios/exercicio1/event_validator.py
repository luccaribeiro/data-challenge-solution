import json
import boto3
import yaml

def send_event_to_queue(event, queue_name):
    _SQS_CLIENT = boto3.client("sqs", region_name="us-east-1")
    
    sqs_client = _SQS_CLIENT
    
    response = sqs_client.get_queue_url(
        QueueName=queue_name
    )
    queue_url = response['QueueUrl']
    
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event)
    )
    
    print(f"Response status code: [{response['ResponseMetadata']['HTTPStatusCode']}]")

TYPE_MAPPING = {
    'string': str,
    'object': dict,
    'integer': int,
    'boolean': bool,
}

def load_schema(path):
    with open(path) as f:
        schema = yaml.safe_load(f)
    return schema

def validate_required(data: dict, required: list, prefix: str = "") -> tuple:
    data_keys = set(data.keys())
    required_keys = set(required)
    
    location = f"{prefix}." if prefix else ""
    
    missing = [f"{location}{field}" for field in sorted(required_keys - data_keys)]
    extra = [f"{location}{field}" for field in sorted(data_keys - required_keys)]
    
    return missing, extra


def validate_properties(data: dict, properties: dict, parent: str = "") -> tuple:
    wrong_types = []
    all_missing = []
    all_extra = []
    
    for column, info in properties.items():
        if column not in data:
            continue
            
        col_name = f"{parent}.{column}" if parent else column
        value = data[column]
        expected_type = TYPE_MAPPING[info["type"]]
        
        if not isinstance(value, expected_type):
            wrong_types.append(col_name)
            continue
        
        if info["type"] == "object":
            missing, extra = validate_required(value, info.get("required", []), col_name)
            all_missing.extend(missing)
            all_extra.extend(extra)
            
            nested_types, nested_missing, nested_extra = validate_properties(
                value, info["properties"], col_name
            )
            wrong_types.extend(nested_types)
            all_missing.extend(nested_missing)
            all_extra.extend(nested_extra)
    
    return wrong_types, all_missing, all_extra


def validate_event(event: dict, schema: dict) -> bool:
    messages = []
    
    missing, extra = validate_required(event, schema["required"])
    wrong_types, nested_missing, nested_extra = validate_properties(event, schema["properties"])
    
    all_missing = missing + nested_missing
    if all_missing:
        messages.append(f"Campos obrigatórios ausentes: {', '.join(all_missing)}")
    
    all_extra = extra + nested_extra
    if all_extra:
        messages.append(f"\nCampos extras não permitidos: {', '.join(all_extra)}")
    
    if wrong_types:
        messages.append(f"\nColunas com tipo errado: {', '.join(wrong_types)}")
    
    if messages:
        error_message = ". ".join(messages)
        print(error_message)
        return False
    
    return True

def handler(event):
    schema = load_schema("desafios/exercicio1/schema.json")
    if validate_event(event, schema):
        print("evento enviado")
        send_event_to_queue(event, 'valid-events-queue')
    else:
        print("Evento inválido, não pode ser enviado")