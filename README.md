# Snowplow Data Products to Google Tag Manager Tag Template

This python script generates a Google Tag Manager Tag template from a Snowplow Data Product ID. It aims to simplify the process of creating custom events and entities by making it simple within the Google Tag Manager UI.

### Disclaimer: This is not supported by Snowplow.

## Steps to create a Tag Template

1. Ensure you have python installed with the requests library.

2. Create a '.env' file in the same directory as your script with the following:

```
ORGANIZATION_ID=XXX
API_KEY_ID=XXX
API_KEY=XXX
```

3. Run the script with the Data Product ID you want to create a tag template as an argument. For example:

   `python3.11 ./main.py YOUR-DATA-PRODUCT-ID`

4. Within Google Tag Manager, go to templates
   
5. Click 'New' under tag templates, and then under the 3 dots in the top-right corner click 'Import'

6. Select the gtm_template.tpl file your script created.

7. Click 'Save' - and you will now be able to use your new tag inside Google Tag Manager

## Limitations

1. Google Tag Manager Tags are limited to having a 100 fields. Data Products that are too big may fail. A field is required for each parameter to an event and entity.
2. This has only been tested using python3.11. The only depedencies on external packages is the requests package.
3. There will likely be bugs.

