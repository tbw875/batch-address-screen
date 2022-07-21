import json
import logging
import os
import pandas as pd
import requests
from tqdm import tqdm
from dotenv import load_dotenv

# Loading environmental variables
# Be sure to update your .env file to include your API Key
load_dotenv()

# Setting logging file
logging.basicConfig(
    filename="./progress.log",
    filemode="w",
    format="%(asctime)s %(processName)s-%(levelname)s: %(message)s",
    encoding="utf8",
    level=logging.INFO,
)

##### ENTER YOUR API KEY
API_KEY = os.getenv("API_KEY")

# READ INPUT CSV
# CSV must have a column header of "address"
print('Note: Input file must contain the column "address"')

input_csv = input("Enter path/to/file: ")
df = pd.read_csv(input_csv)

output_path = input("Enter path and filename for output: ")

# Define header JSON to be used in each API call.
headers = {"token": API_KEY, "Content-Type": "application/json"}

# ITERATE
# Iterate over each row of the CSV file and call the Address Screening API.
# https://docs.chainalysis.com/api/address-screening/#register-an-address
responses = []
print("Registering and evaluating addresses...")
for index, row in tqdm(df.iterrows(), total=df.shape[0]):
    address = row["address"]

    # BUILD REGISTRATION API CALL
    REGISTER_URL = "https://api.chainalysis.com/api/risk/v2/entities"

    # Load parsed inputs into a json object to be used in the payload of the API request
    newPayload = json.dumps({"address": address})

    # Call registration API. Do nothing with it.
    r = requests.request("POST", REGISTER_URL, headers=headers, data=newPayload)

    # BUILD FETCH API CALL
    FETCH_URL = f"https://api.chainalysis.com/api/risk/v2/entities/{address}"

    response = requests.request("GET", FETCH_URL, headers=headers)

    # Write output to `responses` list
    responses.append(json.loads(response.text))

    logging.info("Called address: %s" % address)

logging.info("All API calls finished.")

# In order to correctly flatten the JSON, we need to alter the JSON for those responses where `addressIdentifications` is an empty list. We simply add an empty dict.
# more info: https://stackoverflow.com/questions/63813378/how-to-json-normalize-a-column-in-pandas-with-empty-lists-without-losing-record/63876897#63876897
for i, d in enumerate(responses):
    if not d["addressIdentifications"]:
        responses[i]["addressIdentifications"] = [{}]

# Dropping type None, which represent errored API responses.
data = []
for i in responses:
    if i is not None:
        data.append(i)

print(data)

logging.info("Empty dicts filled")

# Insert `data` into a dataframe
# @notice: There is a bug (#44312) in pandas prior to version 1.4.3 that errors with NoneType at record_path.
df_out = pd.json_normalize(
    data,
    # meta flattens the json including the dictionary of cluster into different columns
    meta=["address", "risk", ["cluster", "name"], ["cluster", "category"]],
    # record path flattens the list of json objects within addressIdentifications
    # @notice: If there are more than one addressIdentifications for one address, pandas will return multiple rows for one address.
    record_path="addressIdentifications",
    # prefix is required because addressIdentifications has the same keynames as the cluster object (name and category)
    record_prefix="addressIdentification_",
    errors="ignore",
)


# Merge input dataframe with user ID with output dataframe from API
df = df.merge(df_out, how="outer", on="address")

# Write to disk.
print(f"Finished! Writing to {output_path}")
logging.info("Writing to disk.")
df.to_csv(output_path, encoding="utf8", index=False)
logging.info("Script is finished.")
