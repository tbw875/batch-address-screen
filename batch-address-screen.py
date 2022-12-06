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
    level=logging.INFO,
)

##### ENTER YOUR API KEY
env_path = ".env"
envExists = os.path.exists(env_path)
if not envExists:
    print("Create a .env file with API_KEY included")
    exit()
API_KEY = os.getenv("API_KEY")

# READ INPUT CSV
# CSV must have a column header of "address"
print('Note: Input file must contain the column "address"')

input_csv = input("Enter path/to/file: ")
df = pd.read_csv(input_csv)

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

    logging.info("Called address: %s", address)

logging.info("All API calls finished.")

# In order to correctly flatten the JSON, we need to alter the JSON for those responses where `addressIdentifications` is an empty list. We simply add an empty dict.
# more info: https://stackoverflow.com/questions/63813378/how-to-json-normalize-a-column-in-pandas-with-empty-lists-without-losing-record/63876897#63876897
# Flattening
for i, d in enumerate(responses):
    if not d["addressIdentifications"]:
        responses[i]["addressIdentifications"] = [{}]

# Dropping type None, which represent errored API responses.
data = []
for i in responses:
    if i is not None:
        data.append(i)

### BUILDING FLATTENED DATAFRAMES ###
# Here we insert `data` into 3 different dataframes in order to flatten each array successfully.
# The three arrays we flatten are [exposures, triggers, addressIdentifications]
# After they are flattened, we merge them, edit their column names, and print to disk.
# @notice: There is a bug (#44312) in pandas prior to version 1.4.3 that errors with NoneType at record_path.

exposures = pd.json_normalize(
    data,
    meta=[
        "address",
        ["exposures"],
        ["cluster", "name"],
        ["cluster", "category"],
        "risk",
    ],
    record_path="exposures",
    errors="ignore",
)

triggers = pd.json_normalize(
    data, meta=["address", ["triggers"]], record_path="triggers"
)

addressIdentifications = pd.json_normalize(
    data,
    meta=["address", ["addressIdentifications"]],
    record_path="addressIdentifications",
    record_prefix="addressId_",
)


### Merging the three flattened dataframes to one
df1 = pd.merge(left=exposures, right=triggers, on=["address", "category"], how="outer")
details = pd.merge(left=df1, right=addressIdentifications, on="address", how="outer")

# Formatting Details csv
cols = [
    "address",
    "risk",
    "cluster.name",
    "cluster.category",
    "ruleTriggered.risk",
    "category",
    "value",
    "percentage",
    "message",
    # "addressId_name",
    # "addressId_category",
    # "addressId_description",
]

details = details[cols]

newCols = [
    "address",
    "risk",
    "cluster.name",
    "cluster.category",
    "exposure.rule.riskLevel",
    "exposure.category",
    "exposure.value",
    "exposure.percentage",
    "exposure.rule.message",
    # "addressId.name",
    # "addressId.category",
    # "addressId.description",
]

details.columns = newCols

# Writing Summary tables
summaryCols = ["address", "risk", "cluster.name", "cluster.category"]
summary = details[summaryCols]
summary.drop_duplicates(subset="address", inplace=True)

exposures = pd.pivot_table(exposures, index="address", columns=["category"])
triggers = pd.pivot_table(triggers, index="address", columns=["category"])

tmp_cols = ["address"]
for i in exposures.columns.tolist():
    tmp_cols.append(i[1])

exposures = pd.DataFrame(exposures.to_records())
exposures.columns = tmp_cols
exposures.fillna(0, inplace=True)
# Exposures is finished here

tmp_cols = ["address"]
for i in triggers.columns.tolist():
    tmp_cols.append(i[1])

triggers = pd.DataFrame(triggers.to_records())
triggers.columns = tmp_cols
# Triggers done

# Writing Summary table
summaryCols = ["address", "risk", "cluster.name", "cluster.category"]
s1 = details[summaryCols]
s1.drop_duplicates(subset="address", keep="first", inplace=True)
summary = pd.merge(s1, exposures, on="address", how="inner")

# Write to disk.
path = "results"
# Check whether the specified path exists or not
isExist = os.path.exists(path)

if not isExist:
    # Create a new directory because it does not exist
    os.mkdir(path)

OUTPUT_PATH = "./results/"
print(f"Finished! Writing SUMMARY and DETAILS tables to {OUTPUT_PATH}")

logging.info("Writing to disk at {OUTPUT_PATH}.")
detailsPath = OUTPUT_PATH + "details.csv"
details.to_csv(detailsPath, encoding="utf8", index=False)

summaryPath = OUTPUT_PATH + "summary.csv"
summary.to_csv(summaryPath, encoding="utf8", index=False)


logging.info("Script is finished.")
