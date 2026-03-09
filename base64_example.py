import hmac, hashlib, base64

# LF-only payload; no trailing newline after closing brace.
content="{\n \"caseId\": \"my customer ID\",\r\n \"name\": \"John Doe\",\r\n \"providerTypes\": [\"WATCHLIST\"]\r\n}"

print(len(content))
data_to_sign = """(request-target): post /screening/v3/groups
host: api.risk.lseg.com
date: Mon, 09 Mar 2026 02:59:04 GMT
""" 

api_secret = ""
# api_token = base64.b64decode(api_secret)
api_token = api_secret.encode()
encoded_data = data_to_sign.encode()

print(data_to_sign)

def generate_hmac_signature(secret, data):
    encrypt = hmac.new(secret, data, digestmod=hashlib.sha256)
    digest_maker = encrypt.digest()
    base = base64.b64encode(digest_maker)
    return base.decode()

hmac_signature = generate_hmac_signature(api_token, encoded_data)
print(hmac_signature)

expected_hmac = "zdD/rjgxGFhz9YSlZ7LHjNei/dy2K5QC8rhilcCq8ek="
print("hmac_signature == expected_hmac:", hmac_signature == expected_hmac)