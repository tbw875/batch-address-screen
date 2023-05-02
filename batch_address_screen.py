import json
import logging
import os
import argparse
import pandas as pd
import requests
from tqdm import tqdm
from dotenv import load_dotenv


### Function Definitions ###


def load_environment_variables():
    """
    Reads in your environment variables that you stored in a .env file within the working directory.
    """
    print("Loading enviroment variables ...")
    load_dotenv()
    env_path = ".env"
    env_exists = os.path.exists(env_path)
    if not env_exists:
        print(
            "Create a .env file in your current working directory with API_KEY included"
        )
        exit()
    api_key = os.getenv("API_KEY")
    return api_key


def read_input_file(csv_path):
    """
    Reads CSV file that is specified as an argument when running the script from command line.
    """
    print("Reading input CSV ...")
    df = pd.read_csv(csv_path)
    return df


def setup_logging():
    """
    Setting up logging configuration for troubleshooting.
    """
    if not os.path.exists("logs"):
        os.makedirs("logs")
    logging.basicConfig(
        filename="logs/progress.log",
        filemode="w",
        format="%(asctime)s %(processName)s-%(levelname)s: %(message)s",
        level=logging.INFO,
    )


def get_headers(api_key):
    headers = {"token": api_key, "Content-type": "application/json"}
    return headers


def process_addresses(df, headers):
    print("Processing addresses through API ...")
    responses = []

    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        address = row["address"]

        register_url = "https://api.chainalysis.com/api/risk/v2/entities"
        new_payload = json.dumps({"address": address})
        requests.request(
            "POST", register_url, headers=headers, data=new_payload, timeout=60
        )

        fetch_url = f"https://api.chainalysis.com/api/risk/v2/entities/{address}"
        response = requests.request("GET", fetch_url, headers=headers, timeout=60)
        responses.append(json.loads(response.text))

        logging.info("HTTP Status: %s for address %s", response.status_code, address)

    logging.info("All API calls finished.")
    return responses


def save_raw_json(responses, file_name="results/responses.json"):
    """
    Saves a JSON file with the results of the API requests.
    If there is an issue parsing results, use this file to prevent having to call the API again.
    """
    if not os.path.exists("results"):
        os.makedirs("results")

    print("Saving JSON ...")
    with open(file_name, "w") as f:
        json.dump(responses, f)


def process_responses(responses):
    print("Parsing responses ...")
    # In order to correctly flatten the JSON, we need to alter the JSON for those responses where `addressIdentifications` is an empty list.
    # We simply add an empty dict.
    for i, d in enumerate(responses):
        if not d["addressIdentifications"]:
            responses[i]["addressIdentifications"] = [{}]

    # Flattening exposures and triggers
    exposures = pd.json_normalize(
        responses,
        meta=["address", "risk", ["cluster", "name"], ["cluster", "category"]],
        record_path="exposures",
        errors="ignore",
    )

    address_identifications = pd.json_normalize(
        responses,
        meta=[
            "address",
            "risk",
            ["cluster", "name"],
            ["cluster", "category"],
            ["addressId", "name"],
            ["addressId", "category"],
            ["addressId", "description"],
        ],
        record_path="addressIdentifications",
        errors="ignore",
    )

    # Pivot the exposures DataFrame
    all_categories = [
        "atm",
        "child abuse material",
        "darknet market",
        "decentralized exchange contract",
        "exchange",
        "fee",
        "fraud shop",
        "gambling",
        "high risk exchange",
        "high risk jurisdiction",
        "hosted wallet",
        "ico",
        "illicit actor-org",
        "infrastructure as a service",
        "lending contract",
        "merchant services",
        "mining",
        "mining pool",
        "mixing",
        "none",
        "online pharmacy",
        "other",
        "p2p exchange",
        "protocol privacy",
        "ransomware",
        "sanctions",
        "scam",
        "smart contract",
        "stolen funds",
        "terrorist financing",
        "token smart contract",
        "unnamed service",
    ]

    exposures_pivoted = exposures.pivot_table(
        values="value", index=["address", "risk"], columns="category", fill_value=0
    ).reset_index()
    exposures_pivoted.columns.name = None

    # Add missing categories to the DataFrame with all values set to 0
    for category in all_categories:
        if category not in exposures_pivoted.columns:
            exposures_pivoted[category] = 0

    # Merge the flattened dataframes
    merged_df = exposures_pivoted.merge(
        address_identifications, on=["address", "risk"], how="outer"
    )

    # Reorder columns
    column_order = [
        "address",
        "risk",
        "cluster.name",
        "cluster.category",
        "addressId.name",
        "addressId.category",
        "addressId.description",
    ] + all_categories

    merged_df = merged_df[column_order]

    return merged_df


def save_output_csv(merged_df):
    # TODO: Write function to save csv to disk.
    # Create a new directory if not already exist
    output_dir = "results"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Save dataframe to csv
    output_path = os.path.join(
        output_dir, "Chainalysis_AddressScreeningAPI_Results.csv"
    )
    merged_df.to_csv(output_path, encoding="utf8", index=False)
    logging.info("Results saved to CSV.")
    print(f"Output saved to {output_path}")


### MAIN function runs the above functions ###


def main():
    setup_logging()

    api_key = load_environment_variables()
    headers = get_headers(api_key)

    # Parser loads in the file from command line argument
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "csv_path",
        help="Enter path/to/file.csv that contains a column called `addresses`",
    )
    args = parser.parse_args()
    df = read_input_file(args.csv_path)

    # Calling the API, saving JSON to disk.
    responses = process_addresses(df, headers)
    save_raw_json(responses)

    # Parsing the results
    processed_data = process_responses(responses)
    save_output_csv(processed_data)


if __name__ == "__main__":
    main()
