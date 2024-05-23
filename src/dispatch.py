import json
import uuid
import boto3
import os
import urllib.request
import urllib.error
import socket

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table_name = os.environ['CONTRACT_TEMPLATES_TABLE_NAME']
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    # Parse the request body
    body = json.loads(event.get('body', '{}'))
    action = body.get('action')
    
    if action == 'quote':
        new_uuid = body.get('uuid')
        token = body.get('token')
        if 'location_service_endpoints' in body:
            datum_name = 'location_service_endpoints'
        if 'ride_matching_service_endpoints' in body:
            datum_name = 'ride_matching_service_endpoints'
        if 'notification_service_endpoints' in body:
            datum_name = 'notification_service_endpoints'
        if 'trip_management_service_endpoints' in body:
            datum_name = 'trip_management_service_endpoints'

        if not datum_name:
            return {
                'statusCode': 400,
                'body': json.dumps('Invalid data')
            }
        datum_content = json.dumps(body[datum_name])
        #
        bid_file_path = f'/tmp/{new_uuid}/{datum_name}.json'
        with open(bid_file_path, 'r') as f:
            bid_data = json.load(f)
            if token != bid_data.get('token'):
                return {
                    'statusCode': 401,
                    'body': json.dumps('Unauthorized')
                }

            if 'endpoints' in bid_data:
                return {
                    'statusCode': 400,
                    'body': json.dumps('Bid already has endpoints')
                }
        bid_data['endpoints'] = datum_content
        with open(bid_file_path, 'w') as f:
            json.dump(bid_data, f)
        #
        return {
            'statusCode': 200,
            'body': json.dumps({})
        }

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
            response = table.query(
                KeyConditionExpression=boto3.dynamodb.conditions.Key('id').eq(stack_id)
            )
            items = response.get('Items', [])
            item = items[0] if items else None
            
            if not item:
                return {
                    'statusCode': 404,
                    'body': json.dumps('stack_id not found in database')
                }
            
            # Generate new UUID and token
            new_uuid = str(uuid.uuid4())
            
            # Extract contract values and endpoints
            services = [
                ('ride_matching_service', 'ride_matching_service_endpoints', 'ride_matching_service_contract_value'),
                ('location_service', 'location_service_endpoints', 'location_service_contract_value'),
                ('notification_service', 'notification_service_endpoints', 'notification_service_contract_value'),
                ('trip_management_service', 'trip_management_service_endpoints', 'trip_management_service_contract_value')
            ]
            
            all_responses = []
            
            dispatch_token = str(uuid.uuid4())
            with open(f'/tmp/{new_uuid}.json', 'w') as f:
                f.write(json.dumps({
                    'uuid': new_uuid,
                    'dispatch_token': dispatch_token
                }))

            if not os.path.exists(f'/tmp/{new_uuid}'):
                os.makedirs(f'/tmp/{new_uuid}')
            for service_name, endpoints_key, contract_value_key in services:
                endpoints_csv = item.get(endpoints_key)
                contract_value = item.get(contract_value_key)
                
                if not endpoints_csv or contract_value is None:
                    continue
                
                endpoints = [e.strip() for e in endpoints_csv.split(',')]
                new_token = str(uuid.uuid4())
                
                # Prepare payload
                payload = json.dumps({
                    'uuid': new_uuid,
                    'token': new_token,
                    'contract_value': float(contract_value),
                    'action': 'request_for_quote',
                    'dispatch_endpoint': os.environ['DISPATCH_ENDPOINT']
                }).encode('utf-8')

                with open(f'/tmp/{new_uuid}/{endpoints_key}.json', 'w') as f:
                    f.write(json.dumps({
                        'uuid': new_uuid,
                        'token': new_token,
                        'contract_value': float(contract_value)
                    }))
                
                headers = {
                    'Content-Type': 'application/json'
                }
                
                # Send requests to each endpoint
                service_responses = []
                for endpoint in endpoints:
                    request = urllib.request.Request(endpoint, data=payload, headers=headers, method='POST')
                    try:
                        with urllib.request.urlopen(request, timeout=2) as response:
                            service_responses.append({
                                'status': response.getcode(),
                                'response': response.read().decode('utf-8')
                            })
                    except urllib.error.URLError as e:
                        service_responses.append({
                            'status': 'error',
                            'error': str(e)
                        })
                    except socket.timeout:
                        service_responses.append({
                            'status': 'error',
                            'error': 'The read operation timed out'
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
