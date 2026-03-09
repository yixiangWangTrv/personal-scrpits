# A simple python web request script to sync screen cases
#
# Code was written from Python requests HTTP library. Use "pip install requests" before executing the script
# Modified: call GET https://api.risk.lseg.com/screening/v3/groups (no params, no body)
# Print: signing text, HTTP status code, HMAC signature value
#
# Irfan Khan 08/13/2025 (original)
# Modified per request

import requests
import hmac
import hashlib
import base64
import time

api_key = ""     # Enter your API key
api_secret = ""  # Enter your API secret

api_token = api_secret.encode()

# Target endpoint (GET, no query, no body)
path = "https://api.risk.lseg.com/screening/v3/groups"
gateway_host = "api.risk.lseg.com"

# For signing text: path must be the path component only (no scheme/host)
path_for_signing = "/screening/v3/groups"


def generate_hmac_signature(secret: bytes, data: bytes) -> str:
    encrypt = hmac.new(secret, data, digestmod=hashlib.sha256)
    digest_maker = encrypt.digest()
    return base64.b64encode(digest_maker).decode()


def get_groups():
    # To get the GMT time string
    date_format = "%a, %d %b %Y %H:%M:%S GMT"
    date_now = time.gmtime()
    date = time.strftime(date_format, date_now)

    # No payload => sign only (request-target), host, date
    datatosign = "(request-target): get " + path_for_signing + "\n" + \
                 "host: " + gateway_host + "\n" + \
                 "date: " + date

    # Print the signing text used to compute the signature
    print(datatosign)

    byte_datatosign = datatosign.encode("utf-8")
    hmacbase = generate_hmac_signature(api_token, byte_datatosign)

    # IMPORTANT: headers list must match what you actually sign
    authorisation = (
        "Signature keyId=\"" + api_key + "\""
        + ",algorithm=\"hmac-sha256\""
        + ",headers=\"(request-target) host date\""
        + ",signature=\"" + hmacbase + "\""
    )

    headers = {
        "Authorization": authorisation,
        "Date": date,
    }

    try:
        result = requests.request("GET", path, headers=headers)
        # Only print status code and HMAC signature value (plus signing text above)
        print(result.status_code)
        print(hmacbase)
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err)


get_groups()