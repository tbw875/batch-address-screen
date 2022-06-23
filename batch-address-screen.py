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

print("Note: Input file must contain the column ['address'] with an optional index")

input_csv = input("Enter path/to/file: ")
df = pd.read_csv(input_csv)

output_path = input("Enter path and filename for output: ")

# Define header JSON to be used in each API call.
headers = {
  'token': api_key,
  'Content-Type': 'application/json'
}

# ITERATE
# Iterate over each row of the CSV file and call the Address Screening API.
# https://docs.chainalysis.com/api/address-screening/#register-an-address
data = []
print("Registering and evaluating addresses...")
for index, row in tqdm(df.iterrows(), total=df.shape[0]):
    address = row['address']

    # BUILD REGISTRATION API CALL
    regUrl = "https://api.chainalysis.com/api/risk/v2/entities"

    # Load parsed inputs into a json object to be used in the payload of the API request
    newPayload = json.dumps(
        {"address": address})

    # Call registration API. Do nothing with it.
    r = requests.request("POST", regUrl, headers=headers, data=newPayload)

    # BUILD FETCH API CALL
    fetchUrl = f"https://api.chainalysis.com/api/risk/v2/entities/{address}"

    response = requests.request("GET", fetchUrl, headers=headers)

    # Write output to `data` list
    data.append(json.loads(response.text))

# Insert `data` into a dataframe
df_out = pd.DataFrame(pd.json_normalize(data))

# Pre-emptively remove `asset` column from input file. Redundant.
df.drop("asset",axis=1,inplace=True)

# Merge input dataframe with user ID with output dataframe from API
df = df.merge(df_out,how="outer",on="address")

# Write to disk.
print(f"Finished! Writing to {output_path}")
df.to_csv(output_path,encoding='utf8',index=False)
