import requests
import json

url = "https://api.chainalysis.com/api/kyt/v1/users/User875/withdrawaladdresses"

payload = {
    "network":"Ethereum"
    ,"asset":"ETH"
    ,"address":"0x92193107FB10B3B372AB21cC90b5a4DBd67861d9"}

headers = {
  'token': '06094f6ef2909569d7533ecd8706cad35703709de49713299d6e74edb937b878',
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response)
