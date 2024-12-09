import requests
import json
import re
import hashlib
import sys


organization_id = None
api_key_id = None
api_key = None

with open('.env', 'r') as f:
    for line in f:
        parts = line.strip().split('=')
        if parts[0] == 'ORGANIZATION_ID':
            organization_id = parts[1]
        elif parts[0] == 'API_KEY_ID':
            api_key_id = parts[1]
        elif parts[0] == 'API_KEY':
            api_key = parts[1]
            
access_token = None

USE_CONTEXT_GENERATOR = False

def get_data_products(data_product_id):
    url = f'https://console.snowplowanalytics.com/api/msc/v1/organizations/{organization_id}/data-products/v2/{data_product_id}'

    headers = {
        'authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        with open('./output/data_product.json', 'w') as f:
            json.dump(response.json(), f)

        return response.json()
    else:
        print(f'Error: {response.status_code}')

def generate_schema_hash(schema):
    schema_parts = re.split('[:/]', schema)
    schema_hash = '-'.join(schema_parts[1:-1])
    schema_hash = f'{organization_id}-' + schema_hash
    schema_hash = hashlib.sha256(schema_hash.encode()).hexdigest()
    return schema_hash

def get_schema(schema):
    schema_hash = generate_schema_hash(schema)
    url = f'https://console.snowplowanalytics.com/api/msc/v1/organizations/{organization_id}/data-structures/v1/{schema_hash}'
    headers = {'authorization': f'Bearer {access_token}'}
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f'Error: {response.status_code}')
    
    response_json = response.json()

    latest_version = response_json['deployments'][-1]['version']

    url = f'https://console.snowplowanalytics.com/api/msc/v1/organizations/{organization_id}/data-structures/v1/{schema_hash}/versions/{latest_version}'
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f'Error: {response.status_code}')
    
    return response.json()

def get_api_token(organization_id, api_key_id, api_key):
    url = f'https://console.snowplowanalytics.com/api/msc/v1/organizations/{organization_id}/credentials/v3/token'

    headers = {
        'X-API-Key-ID': api_key_id,
        'X-API-Key': api_key
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()['accessToken']
    else:
        print(f'Error: {response.status_code}')
        return None
    
def create_gtm_template_parameters(data_product_json):
    event_specs = data_product_json['includes']['eventSpecs']
    select_items = []
    event_spec_parameters = []
    entities_processed = []
    entity_event_map = {}

    for event_spec in event_specs:
        select_items.append({
            'value': event_spec['name'],
            'displayValue': event_spec['name']
        })

        sub_parameters = []

        event_spec_event_schema = get_schema(event_spec['event']['source'])

        # if not 'schema' in event_spec['event']:
        #     event_spec['event']['schema'] = get_schema(event_spec['event']['source'])

        for property in event_spec_event_schema['properties']:
            if 'schema' in event_spec['event'] and property in event_spec['event']['schema']['properties']:
                property_object = event_spec['event']['schema']['properties'][property]
            else:
                property_object = event_spec_event_schema['properties'][property]

            display_name = f'{property}'

            if 'enum' in property_object:
                display_name += f" ({property_object['enum']})"
                sub_parameters.append({
                    "type": "SELECT", 
                    "name": f"{event_spec['name']}|{property}",
                    "displayName": property,
                    "selectItems": [{'value': item, 'displayValue': item} for item in property_object['enum']],
                    "simpleValueType": True
                })
            else:
                sub_parameters.append({
                    "type": "TEXT",
                    "name": f"{event_spec['name']}|{property}",
                    "displayName": property + f" ({property_object['type']})",
                    "simpleValueType": True
                })

        entity_parameters = []
        if 'entities' in event_spec and 'tracked' in event_spec['entities']:
            for entity in event_spec['entities']['tracked']:
                if entity['source'] in entity_event_map:
                    entity_event_map[entity['source']].append(event_spec['name'])
                else:
                    entity_event_map[entity['source']] = [event_spec['name']]

        event_spec_enabling_condition = [{
            "paramName": "eventSpec",
            "paramValue": event_spec['name'],
            "type": "EQUALS"
        }]

        event_spec_parameters.append({
            "type": "GROUP",
            "name": event_spec['name'],
            "displayName": f"{event_spec['name']} Event Parameters",
            "groupStyle": "ZIPPY_OPEN",
            "subParams": sub_parameters,
            "enablingConditions": event_spec_enabling_condition
        })

        # event_spec_parameters.append({
        #     "type": "GROUP",
        #     "name": event_spec['name'] + '_entities',
        #     "displayName": f"{event_spec['name']} Entities",
        #     "groupStyle": "ZIPPY_OPEN",
        #     "subParams": entity_parameters,
        #     "enablingConditions": enabling_condition
        # })

    for entity in entity_event_map:
        entity_parameters = []
        source_parts = '/'.join(entity.split('/')[1:])
        entity_schema = get_schema(entity)

        entity_parameters.append({
            "type": "LABEL",
            "name": f"{entity_schema['self']['name']}_context_generator_label",
            "displayName": f"Use below to create multiple entities using a Custom JavaScript variable."
        })
        entity_parameters.append({
            "type": "SELECT",
            "name": f"{entity_schema['self']['name']}|context_generator",
            "displayName": f"Context Generator for {source_parts}",
            "macrosInSelect": True,
            "selectItems": [
            {
                "value": "no",
                "displayValue": "No"
            }
            ],
            "simpleValueType": True,
            "help": "Set this to a Google Tag Manager variable that returns the function you want to execute when assigning a custom context to this event specification."
        })

        entity_parameters.append({
            "type": "LABEL",
            "name": f"{entity_schema['self']['name']}_fields_label",
            "displayName": f"Use the fields below to create a single entity."
        })

        for property in entity_schema['properties']:
            display_name = property
            property_object = entity_schema['properties'][property]

            if 'enum' in property_object:
                display_name += f" ({property_object['enum']})"
                entity_parameters.append({
                    "type": "SELECT", 
                    "name": f"{entity_schema['self']['name']}|{property}",
                    "displayName": property,
                    "selectItems": [{'value': item, 'displayValue': item} for item in property_object['enum']],
                    "simpleValueType": True,
                    "macrosInSelect": True
                })
            else:
                entity_parameters.append({
                    "type": "TEXT",
                    "name": f"{entity_schema['self']['name']}|{property}",
                    "displayName": property + f" ({property_object['type']})",
                    "simpleValueType": True
                })

        entity_enabling_condition = []
        for event_spec in entity_event_map[entity]:
            entity_enabling_condition.append({
                "paramName": "eventSpec",
                "paramValue": event_spec,
                "type": "EQUALS"
            })
        
        event_spec_parameters.append({
            "type": "GROUP",
            "name": entity_schema['self']['name'] + '_entities',
            "displayName": f"{entity_schema['self']['name']} Entities",
            "groupStyle": "ZIPPY_OPEN",
            "subParams": entity_parameters,
            "enablingConditions": entity_enabling_condition
        })

    output = [{
        "type": "SELECT",
        "name": "eventSpec",
        "displayName": "Select an event spec",
        "macrosInSelect": False,
        "selectItems": select_items,
        "simpleValueType": True
    },
    *event_spec_parameters]

    with open('./output/gtm_template_parameters.json', 'w') as f:
        json.dump(output, f)

    event_entity_map = {}
    for entity in entity_event_map:
        for event in entity_event_map[entity]:
            entity_name = re.split('[:/]', entity)[2]
            if event in event_entity_map:
                event_entity_map[event].append(entity_name)
            else:
                event_entity_map[event] = [entity_name]
    
    return data_product_json,event_entity_map

def convert_to_camel_case(text):
    return ''.join([x.capitalize() for x in re.split('[_ -]', text)])

# Creates the GTM template code based on the data product JSON
# The code should be a switch statement that calls the appropriate function
# The function call should be in the format: callInWindow('__snowtype.trackXXX', {'button_label':'test'});
# Where the XXX is event spec name without spaces and the event source converted to camel case
# Example:
# switch (data.eventSpec) {
#   case 'Bad Button Click':
#     callInWindow('__snowtype.trackJbCustomButtonClickBadButtonClick',
#                  {'button_label':'test'});
# }
def create_gtm_template_code(data_product_json,event_entity_map):

    event_specs = data_product_json['includes']['eventSpecs']
    event_entity_map_json = json.dumps(event_entity_map)
    permission_keys = []
    output_code = f'''
        // Enter your template code here.
        const log = require('logToConsole');
        log('data =', data);

        // Call data.gtmOnSuccess when the tag is finished.
        data.gtmOnSuccess();

        var event_entity_map = {event_entity_map_json};

        const callInWindow = require('callInWindow');
        switch (data.eventSpec)
    ''' + '{'
    for event_spec in event_specs:
        event_spec_name = event_spec['name']

        event_spec_name_camel_case = convert_to_camel_case(event_spec_name)

        event_source = event_spec['event']['source']
        event_source_parts = event_source.split('/')
        event_source_camel_case = convert_to_camel_case(event_source_parts[1])

        event_spec_event_schema = get_schema(event_spec['event']['source'])
        
        properties_code = '{'
        for property in event_spec_event_schema['properties']:
            properties_code += f"'{property}':data['{event_spec_name}|{property}'],"
        properties_code += 'context: context}'

        context_code = ''
        for entity in event_entity_map[event_spec_name]:
            context_code += f"data['{entity}|context_generator'] != 'no' ? context = context.concat(data['{entity}|context_generator']) : null;\n"

        formatted_code = f"case '{event_spec_name}':\n\
            var context = [];\n\
            {context_code}\n\
            callInWindow('__snowtype.track{event_source_camel_case}{event_spec_name_camel_case}',\n\
            {properties_code});break;\n"

        persmissions_key_str = '{"type":3,"mapKey":[{"type":1,"string":"key"},{"type":1,"string":"read"},{"type":1,"string":"write"},{"type":1,"string":"execute"}],"mapValue":[{"type":1,"string":"' + f'__snowtype.track{event_source_camel_case}{event_spec_name_camel_case}' + '"},{"type":8,"boolean":true},{"type":8,"boolean":true},{"type":8,"boolean":true}]}'
        
        permission_keys.append(persmissions_key_str)

        output_code += formatted_code

    output_code += '}\n'

    with open('./output/gtm_template_code.js', 'w') as f:
        f.write(output_code)

    return permission_keys

def create_gtm_template_permissions(permission_keys):
    
    with open('./permissions_template.json', 'r') as f:
        permissions_template = json.load(f)
    
    for key in permission_keys:
        permissions_template[1]['instance']['param'][0]['value']['listItem'].append(json.loads(key))

    with open('./output/gtm_template_permissions.json', 'w') as f:
        json.dump(permissions_template, f)

# Read in all the GTM template files and combine them into a single file
def combine_gtm_template_files(data_product_json):

    with open('./output/gtm_template_parameters.json', 'r') as f:
        gtm_template_parameters = json.load(f)
    
    with open('./output/gtm_template_code.js', 'r') as f:
        gtm_template_code = f.read()

    with open('./output/gtm_template_permissions.json', 'r') as f:
        gtm_template_permissions = json.load(f)
    
    data_product_names = ','.join([x['name'] for x in data_product_json['data']])
    output = '''
            ___INFO___

        {
        "type": "TAG",
        "id": "cvt_temp_public_id",
        "version": 1,
        "securityGroups": [],
        "displayName": "''' + f"Snowtype GTM Tag Template for {data_product_names}" +  '''",
        "brand": {
            "id": "brand_dummy",
            "displayName": ""
        },
        "description": "'''+ f"A custom template for Snowtype data products based on {data_product_names}" + '''",
        "containerContexts": [
            "WEB"
        ]
        }
        
        ___TEMPLATE_PARAMETERS___
        
        ''' + json.dumps(gtm_template_parameters) + '''

        ___SANDBOXED_JS_FOR_WEB_TEMPLATE___

        ''' + gtm_template_code + '''

        ___WEB_PERMISSIONS___

        ''' + json.dumps(gtm_template_permissions)
    
    with open('./output/gtm_template.tpl', 'w') as f:
        f.write(output)




def run_template_creation(data_product_id):
    data_product_json = get_data_products(data_product_id)
    data_product_json,event_entity_map = create_gtm_template_parameters(data_product_json)
    permission_keys = create_gtm_template_code(data_product_json,event_entity_map)
    create_gtm_template_permissions(permission_keys)
    combine_gtm_template_files(data_product_json)

if __name__ == '__main__':
    access_token = get_api_token(organization_id, api_key_id, api_key)

    data_product_id = 'd5f1fbb3-e03a-4f31-be7d-a5ca15c98fa3' # Growth
    #data_product_id = 'b6ace794-980f-42e1-8c17-51441111c912' # Media
    #data_product_id = 'a42cc8e6-7ef9-4433-853d-1c23995f4afe' # JB test
    #data_product_id = 'ff5bd446-25eb-4aaf-bb8e-ded18b2fff15'

    run_template_creation(data_product_id)
    
