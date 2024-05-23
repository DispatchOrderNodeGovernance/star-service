import json
import uuid
import boto3
import os
import requests

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table_name = os.environ['CONTRACT_TEMPLATES_TABLE_NAME']
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    # Parse the request
    action = event.get('action')
    
    if action == 'dispatch':
        # Extract stack_id from the request
        stack_id = event.get('stack_id')
        
        if not stack_id:
            return {
                'statusCode': 400,
                'body': json.dumps('Missing stack_id in request')
            }
        
        # Query the database for the given stack_id
        try:
            response = table.get_item(Key={'id': stack_id})
            item = response.get('Item')
            
            if not item:
                return {
                    'statusCode': 404,
                    'body': json.dumps('stack_id not found in database')
                }
            
            # Generate new UUID and token
            new_uuid = str(uuid.uuid4())
            new_token = str(uuid.uuid4())
            
            # Extract contract values and service endpoints
            contract_values = {
                'ride_matching_service_contract_value': item.get('ride_matching_service_contract_value'),
                'location_service_contract_value': item.get('location_service_contract_value'),
                'notification_service_contract_value': item.get('notification_service_contract_value'),
                'trip_management_service_contract_value': item.get('trip_management_service_contract_value')
            }
            
            endpoints = {
                'ride_matching_service_endpoints': item.get('ride_matching_service_endpoints'),
                'location_service_endpoints': item.get('location_service_endpoints'),
                'notification_service_endpoints': item.get('notification_service_endpoints'),
                'trip_management_service_endpoints': item.get('trip_management_service_endpoints')
            }
            
            if not all(contract_values.values()) or not all(endpoints.values()):
                return {
                    'statusCode': 500,
                    'body': json.dumps('Missing contract values or endpoints in database item')
                }
            
            # Prepare payloads and send requests
            responses = []
            for service, contract_value in contract_values.items():
                endpoint_key = service.replace('contract_value', 'endpoints')
                endpoint_list = endpoints.get(endpoint_key)
                
                if endpoint_list:
                    payload = {
                        'uuid': new_uuid,
                        'token': new_token,
                        'contract_value': contract_value
                    }
                    
                    for endpoint in endpoint_list:
                        try:
                            response = requests.post(endpoint, json=payload, timeout=0.1)
                            responses.append({'service': service, 'status': response.status_code, 'response': response.text})
                        except requests.exceptions.RequestException as e:
                            responses.append({'service': service, 'status': 'error', 'error': str(e)})
            
            return {
                'statusCode': 200,
                'body': json.dumps({'results': responses})
            }
        
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps(f'Error querying database: {str(e)}')
            }
    
    return {
        'statusCode': 400,
        'body': json.dumps('Invalid action')
    }
