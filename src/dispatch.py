import json
import uuid
import boto3
import os
import urllib.request
import urllib.error

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table_name = os.environ['CONTRACT_TEMPLATES_TABLE_NAME']
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    # Parse the request body
    body = json.loads(event.get('body', '{}'))
    action = body.get('action')
    
    if action == 'dispatch':
        # Extract stack_id from the request body
        stack_id = body.get('stack_id')
        
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
            
            # Extract contract values and endpoints
            services = [
                ('ride_matching_service', 'ride_matching_service_endpoints', 'ride_matching_service_contract_value'),
                ('location_service', 'location_service_endpoints', 'location_service_contract_value'),
                ('notification_service', 'notification_service_endpoints', 'notification_service_contract_value'),
                ('trip_management_service', 'trip_management_service_endpoints', 'trip_management_service_contract_value')
            ]
            
            all_responses = []
            
            for service_name, endpoints_key, contract_value_key in services:
                endpoints_csv = item.get(endpoints_key)
                contract_value = item.get(contract_value_key)
                
                if not endpoints_csv or contract_value is None:
                    continue
                
                endpoints = [e.strip() for e in endpoints_csv.split(',')]
                
                # Prepare payload
                payload = json.dumps({
                    'uuid': new_uuid,
                    'token': new_token,
                    'contract_value': float(contract_value)
                }).encode('utf-8')
                
                headers = {
                    'Content-Type': 'application/json'
                }
                
                # Send requests to each endpoint
                service_responses = []
                for endpoint in endpoints:
                    request = urllib.request.Request(endpoint, data=payload, headers=headers, method='POST')
                    try:
                        with urllib.request.urlopen(request, timeout=0.1) as response:
                            service_responses.append({
                                'status': response.getcode(),
                                'response': response.read().decode('utf-8')
                            })
                    except urllib.error.URLError as e:
                        service_responses.append({
                            'status': 'error',
                            'error': str(e)
                        })
                
                all_responses.append({
                    'service': service_name,
                    'results': service_responses
                })
            
            return {
                'statusCode': 200,
                'body': json.dumps({'results': all_responses})
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
