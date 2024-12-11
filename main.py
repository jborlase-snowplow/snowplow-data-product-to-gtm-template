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

# Get the data products for the organization
def get_data_products(data_product_id):
    url = f'https://console.snowplowanalytics.com/api/msc/v1/organizations/{organization_id}/data-products/v2/{data_product_id}'

    headers = {'authorization': f'Bearer {access_token}'}

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f'Error: {response.status_code}')

# Generate a hash of the schema to use as a key required by the Snowplow API
def generate_schema_hash(schema):
    schema_parts = re.split('[:/]', schema)
    schema_hash = '-'.join(schema_parts[1:-1])
    schema_hash = f'{organization_id}-' + schema_hash
    schema_hash = hashlib.sha256(schema_hash.encode()).hexdigest()
    return schema_hash

# Get the schema for a given Iglu URL - Doesn't work for Iglu Central schemas as they are not in the Snowplow Console
def get_schema(schema):
    schema_hash = generate_schema_hash(schema)
    url = f'https://console.snowplowanalytics.com/api/msc/v1/organizations/{organization_id}/data-structures/v1/{schema_hash}'
    headers = {'authorization': f'Bearer {access_token}'}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        response_json = response.json()
        latest_version = response_json['deployments'][-1]['version']

        url = f'https://console.snowplowanalytics.com/api/msc/v1/organizations/{organization_id}/data-structures/v1/{schema_hash}/versions/{latest_version}'
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f'Error: {response.status_code}')

    elif response.status_code == 404:
        print('Schema not found in Snowplow Console - trying Iglu Central')
        # Try Iglu Central
        schema = schema.removeprefix('iglu:')
        url = f'http://iglucentral.com/schemas/{schema}'
        print(url)
        response = requests.get(url)

        if response.status_code != 200:
            print('Error: Schema not found in Iglu Central')
            return None
        
    elif response.status_code != 200:
        print(f'Error: {response.status_code}')
        return None
    
    return response.json()

def fetch_schemas_from_data_product(data_product_json):
    event_specs = data_product_json['includes']['eventSpecs']
    entity_event_map = {}
    entity_lookup = {}
    

    for event_spec in event_specs:
        event_spec_event_schema = get_schema(event_spec['event']['source'])

        for property in event_spec_event_schema['properties']:
            if 'schema' in event_spec['event'] and property in event_spec['event']['schema']['properties']:
                property_object = event_spec['event']['schema']['properties'][property]
            else:
                property_object = event_spec_event_schema['properties'][property]
        
        event_spec['event']['schema'] = event_spec_event_schema # Add the schema to the event spec in the data product JSON

        if 'entities' in event_spec and 'tracked' in event_spec['entities']:
            for entity in event_spec['entities']['tracked']:
                if entity['source'] in entity_event_map:
                    entity['schema'] = entity_lookup[entity['source']]
                    entity_event_map[entity['source']].append(event_spec['name'])
                else:
                    entity['schema'] = get_schema(entity['source'])
                    entity_lookup[entity['source']] = entity['schema']
                    entity_event_map[entity['source']] = [event_spec['name']]
    
    with open('./output/data_product.json', 'w') as f:
            json.dump(data_product_json, f)

    return data_product_json

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
    entity_event_map = {}
    entity_schemas = {}

    for event_spec in event_specs:
        select_items.append({
            'value': event_spec['name'],
            'displayValue': event_spec['name']
        })

        sub_parameters = []

        event_spec_event_schema = event_spec['event']['schema']

        for property in event_spec_event_schema['properties']:
            property_object = event_spec['event']['schema']['properties'][property]
            display_name = f'{property}'
            required = True if 'required' in event_spec_event_schema and property in event_spec_event_schema['required'] else False
            
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
                    "displayName": property + f" ({property_object['type']})" + (' * Required ' if required else ''),
                    "simpleValueType": True
                })

        entity_parameters = []
        if 'entities' in event_spec and 'tracked' in event_spec['entities']:
            for entity in event_spec['entities']['tracked']:
                if entity['source'] in entity_event_map:
                    entity_event_map[entity['source']].append(event_spec['name'])
                    entity_schemas[entity['source']] = entity['schema']
                else:
                    entity_schemas[entity['source']] = entity['schema']
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

    for entity in entity_event_map:
        entity_parameters = []
        source_parts = '/'.join(entity.split('/')[1:])
        entity_schema = entity_schemas[entity]
        
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
            "displayName": f"Use the fields below to create a single entity. * Required fields"
        })

        for property in entity_schema['properties']:
            display_name = property
            property_object = entity_schema['properties'][property]
            required = True if 'required' in entity_schema and property in entity_schema['required'] else False

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
                    "displayName": property + f" ({property_object['type']})" + (' * ' if required else ''),
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

        event_spec_event_schema = event_spec['event']['schema']

        # var eventSpecificationContext = createEventSpecification({
        #                 id: '06bb7b2b-6d18-4fd8-bbaf-6517d1caf789',
        #                 name: 'Free Trial Signup',
        #                 data_product_id: 'd5f1fbb3-e03a-4f31-be7d-a5ca15c98fa3',
        #                 data_product_name: 'Growth Marketing - SaaS (ProService Demo)'
        #         });

        event_spec_context_data = f"{{id: '{event_spec['id']}',name: '{event_spec_name}',data_product_id: '{data_product_json['data'][0]['id']}',data_product_name: '{data_product_json['data'][0]['name']}'}}"
        event_spec_context_schema = 'iglu:com.snowplowanalytics.snowplow/event_specification/jsonschema/1-0-2'
        event_spec_context = f"[{{data: {event_spec_context_data},schema: '{event_spec_context_schema}'}}]"
        
        # Generate JavaScript code to create the properties object
        properties_code = '{'
        for property in event_spec_event_schema['properties']:
            properties_code += f"'{property}':data['{event_spec_name}|{property}'],"
        properties_code += '}'

        # Generate JavaScript code to create the context object for a single entity
        event_spec_entities = [x for x in event_specs if x['name'] == event_spec['name']][0]['entities']['tracked']
        manual_context_code = ''
        for entity in event_spec_entities:
            entity_name = entity['schema']['self']['name']
            manual_context_code += f'var {entity_name}_context = ' + '{};'
            for property in entity['schema']['properties']:
                manual_context_code += f"data['{entity_name}|{property}'] != 'undefined' ? {entity_name}_context['{property}'] = data['{entity_name}|{property}'] : null;\n"

        # Generate JavaScript code to create the context object for multiple entities
        context_code = ''
        for entity in event_entity_map[event_spec_name]:
            entity_source = [x for x in event_spec_entities if x['schema']['self']['name'] == entity][0]['source']
            context_code += f"data['{entity}|context_generator'] != 'no' ? context = context.concat(data['{entity}|context_generator']) : context = context.concat([{{'schema': '{entity_source}','data': {entity}_context}}]);\n"

        # Format the code into a switch statement case
        formatted_code = f"case '{event_spec_name}':\n\
            var context = {event_spec_context};\n\
            {manual_context_code};\n\
            {context_code}\n\
            callInWindow('snowplow','trackSelfDescribingEvent',\n\
            {{event: {{data:{properties_code},schema:'{event_source}'}}, 'context': context}});break;\n"

        persmissions_key_str = '{"type":3,"mapKey":[{"type":1,"string":"key"},{"type":1,"string":"read"},{"type":1,"string":"write"},{"type":1,"string":"execute"}],"mapValue":[{"type":1,"string":"' + f'__snowtype.track{event_source_camel_case}{event_spec_name_camel_case}' + '"},{"type":8,"boolean":true},{"type":8,"boolean":true},{"type":8,"boolean":true}]}'
        permission_keys.append(persmissions_key_str)

        output_code += formatted_code

    output_code += '}\n'

    persmissions_key_str = '{"type":3,"mapKey":[{"type":1,"string":"key"},{"type":1,"string":"read"},{"type":1,"string":"write"},{"type":1,"string":"execute"}],"mapValue":[{"type":1,"string":"' + f'snowplow' + '"},{"type":8,"boolean":true},{"type":8,"boolean":true},{"type":8,"boolean":true}]}'
    permission_keys.append(persmissions_key_str)

    with open('./output/gtm_template_code.js', 'w') as f:
        f.write(output_code)

    return permission_keys

# Create the permissions JSON for the GTM template
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
        "displayName": "''' + f"Snowplow GTM Tag Template for {data_product_names}" +  '''",
        "brand": {
            "id": "brand_dummy",
            "displayName": ""
        },
        "description": "'''+ f"A custom template for a Snowplow Data Product based on {data_product_names}" + '''",
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


# Run the entire process of creating the GTM template
def run_template_creation(data_product_id):
    data_product_json = get_data_products(data_product_id)
    data_product_json = fetch_schemas_from_data_product(data_product_json)
    data_product_json,event_entity_map = create_gtm_template_parameters(data_product_json)
    permission_keys = create_gtm_template_code(data_product_json,event_entity_map)
    create_gtm_template_permissions(permission_keys)
    combine_gtm_template_files(data_product_json)

if __name__ == '__main__':
    access_token = get_api_token(organization_id, api_key_id, api_key)

    #data_product_id = 'd5f1fbb3-e03a-4f31-be7d-a5ca15c98fa3' # Growth
    #data_product_id = 'b6ace794-980f-42e1-8c17-51441111c912' # Media
    #data_product_id = 'a42cc8e6-7ef9-4433-853d-1c23995f4afe' # JB test
    data_product_id = 'ff5bd446-25eb-4aaf-bb8e-ded18b2fff15' # JB ecommerce demo
    data_product_id = 'b675cefe-3be1-4d55-9538-c017c4dd6f3f'

    # output =  (fetch_schemas_from_data_product(get_data_products(data_product_id)))

    run_template_creation(data_product_id)
    
