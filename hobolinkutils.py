import requests, sys, json
def get_new_token(auth_server_url, client_id, client_secret):
    """Obtain a new OAuth 2.0 token from the authentication server."""
    token_req_payload = {'grant_type': 'client_credentials'}

    token_response = requests.post(auth_server_url,
                                   data=token_req_payload,
                                   verify=False,
                                   allow_redirects=False,
                                   auth=(client_id, client_secret)
                                   )

    if token_response.status_code != 200:
        print("Failed to obtain token from the OAuth 2.0 server")
        sys.exit(1)

    tokens = json.loads(token_response.text)
    return tokens['access_token']