## Rakuten API


import requests
import json
import pandas
from oauth2client.service_account import ServiceAccountCredentials

print('Rakuten API')

#Auth
client_id 		= ''
client_secret 	= ''
token_request 	= ''
acess_token		= ''



response = requests.get(
## ADD Rakuten Link Here
)

with open('C:/BI/Atualizacao_Incremental/Rakuten_API.csv', 'wb') as f:
    f.write(response.content)


