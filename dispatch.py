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
            
            # Extract contract_value and vendor endpoints
            contract_value = item.get('contract_value')
            endpoints_a = item.get('endpoints_a')
            endpoints_b = item.get('endpoints_b')
            
            if not (contract_value and endpoints_a and endpoints_b):
                return {
                    'statusCode': 500,
                    'body': json.dumps('Missing contract value or endpoints in database item')
                }
            
            # Prepare payload
            payload = {
                'uuid': new_uuid,
                'token': new_token,
                'contract_value': contract_value
            }
            
            # Send requests to each endpoint
            responses = []
            for endpoint in [endpoints_a, endpoints_b]:
                try:
                    response = requests.post(endpoint, json=payload)
                    responses.append({'endpoint': endpoint, 'status': response.status_code, 'response': response.text})
                except requests.exceptions.RequestException as e:
                    responses.append({'endpoint': endpoint, 'status': 'error', 'error': str(e)})
            
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
