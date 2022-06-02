import pandas as pd
import getpass
import json
import requests
from tqdm import tqdm

##### ENTER YOUR API KEY
api_key = getpass.getpass("Enter your API Key:")

# READ INPUT CSV
# CSV must have the following columns:
# ['user_id','asset','address']

print("Note: Input file must contain the columns ['user_id','asset','address']")

input_csv = input("Enter path/to/file: ")
df = pd.read_csv(input_csv)

output_path = input("Enter path and filename for output: ")


headers = {
  'token': api_key,
  'Content-Type': 'application/json'
}

# ITERATE
# Iterate over each row of the CSV file and call the Address Screening API.
# https://docs.chainalysis.com/api/address-screening/#register-an-address
data = []
for index, row in tqdm(df.iterrows(), total=df.shape[0]):
    user = row['user_id']
    asset = row['asset']
    address = row['address']

    # BUILD API CALL
    url = f"https://api.chainalysis.com/api/kyt/v1/users/{user}/withdrawaladdresses"
    print(url)
    # Load parsed inputs into a json object to be used in the payload of the API request
    newPayload = json.dumps([
        {"asset": asset,
        "address": address}])

    response = requests.request("POST", url, headers=headers, data=newPayload)
    # API response is a list. .pop() makes it a true JSON
    # `data` is now a list of JSON objects
    data.append(json.loads(response.text).pop())

# Insert `data` into a dataframe
df_out = pd.DataFrame(pd.json_normalize(data))

# Pre-emptively remove `asset` column from input file. Redundant.
df.drop("asset",axis=1,inplace=True)

# Merge input dataframe with user ID with output dataframe from API
df = df.merge(df_out,how="outer",on="address")

# Write to disk.
print(f"Finished! Writing to {output_path}")
df.to_csv(output_path,encoding='utf8',index=False)
