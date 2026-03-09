# A simple python web request script to sync screen cases

# Code was written from Python requests HTTP library. Use "pip install requests" before executing the script
# A POST request to test the connectivity, authorization to the World-Check One API webserver and to synchronously screen cases using the WC1 API
# The screeningRequest endpoint returns all the matches found for the screening entity name
# This script can sync screen cases with entity names containing special characters
# Irfan Khan 08/13/2025


import requests
import hmac
import hashlib
import base64
import time
import json
import io
import pandas as pd

null = None
false = None


api_key = ""  # Enter your API key
api_secret = ""  # Enter your API secret
group_id = ""  # Enter your group Id
api_token = api_secret.encode()
path = "https://api.risk.lseg.com/screening/v3/cases?screen=SYNC"
gateway_url = "/screening/v3/"
gateway_host = "api.risk.lseg.com"
content_type = "application/json"

def screening():
    entity_type = 'INDIVIDUAL'  # Change the entity type to "ORGANISATION" to screen an organisation. The script can handle both entity types
    name = "قاسم قاسم عبدالله هشام"  # Change the name to screen different entity. The script can handle special characters in the name string as well

    # To get the GMT time string
    date_format = "%a, %d %b %Y %H:%M:%S GMT"
    date_now = time.gmtime()
    date = time.strftime(date_format, date_now)
    print(date)

    if entity_type == "INDIVIDUAL":
        payload = {
            "caseId": "",
            "groupId": group_id,
            "entityType": "INDIVIDUAL",
            "providerTypes": [
                "WATCHLIST"
            ],
            "name": name,
            "secondaryFields": [

            ]
        }


    else:
        payload =  {
        "caseId": "",
        "groupId": group_id,
        "entityType": "ORGANISATION",
        "providerTypes": [
            "WATCHLIST"
        ],
        "name": name,
        "secondaryFields": []
    }


    content = json.dumps(payload,
                         ensure_ascii=False,
                         separators=(',', ':')).encode('utf-8')
    str_length = str(len(content))

    datatosign = "(request-target): post " + gateway_url + "cases\n" + \
                 "host: " + gateway_host + "\n" + \
                 "date: " + date + "\n" + \
                 "content-type: " + content_type + "\n" + \
                 "content-length: " + str_length + "\n" + \
                 json.dumps(payload, ensure_ascii=False, separators=(',', ':')) #

    #print(datatosign)

    byte_datatosign = datatosign.encode()


    def hbase(byte_datatosign, api_token):
        encrypt = hmac.new(api_token, byte_datatosign, digestmod=hashlib.sha256)
        digest_maker = encrypt.digest()
        base = base64.b64encode(digest_maker)
        return base.decode()


    hmacbase = hbase(byte_datatosign, api_token)
    #print(hmacbase)

    authorisation = "Signature keyId=\"" + api_key + "\"" + ",algorithm=\"hmac-sha256\"" + ",headers=\"(request-target) host date content-type content-length\"" + ",signature=\"" + hmacbase + "\""
    #print(authorisation)

    headers = {

        'Authorization': authorisation,
        'Date': date,
        'Content-Type': content_type,
        'Content-Length': str_length
    }
    #print(headers)

    try:
        result = requests.request("POST", path, headers=headers, data=content)
        result.raise_for_status()
        print(result.status_code)
        #print(result.headers)
        result_json = result.json()
        #print(result_json['caseSystemId'])
        if result_json['outstandingActions']:
            count = 0
            for result in result_json['results']:
                count +=1
        else:
            count = 0


        # Save a .txt file with the result json to inspect the response from the API.
        # The file will be saved in the same directory as the script

        with io.open("result_json.txt", 'w', encoding="utf-8") as outfile:
            outfile.write(str(json.dumps(result_json, ensure_ascii=False, indent=4)))
        print(result_json)


        return {
                    'caseSystemId': result_json['caseSystemId'],
                    'count': count

                }


    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err)

    return {
                'caseSystemId': 'XXXXXXX',
                'count': 'XXXXXXX'
            }



print(screening())