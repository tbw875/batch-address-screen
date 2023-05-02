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

    # Helper function to flatten nested dictionaries
    def flatten_dict(d, parent_key="", sep="_"):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    # Flatten nested dictionaries in the JSON
    flattened_responses = []
    for response in responses:
        if not response["addressIdentifications"]:
            response["addressIdentifications"] = [{}]

        for address_id in response["addressIdentifications"]:
            new_response = dict(response)
            new_response["addressIdentifications"] = address_id
            flattened_responses.append(flatten_dict(new_response))

    # Create DataFrame from flattened JSON
    df = pd.DataFrame(flattened_responses)

    # Create a dictionary for exposure values
    exposure_values = {}
    for response in responses:
        exposure_values[response["address"]] = {
            exposure["category"]: exposure["value"]
            for exposure in response["exposures"]
        }

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

    # Populate exposure categories
    for category in all_categories:
        df[category] = df.apply(
            lambda row: exposure_values[row["address"]][category]
            if category in exposure_values[row["address"]]
            else 0,
            axis=1,
        )

    # Drop exposures column
    df = df.drop("exposures", axis=1)

    # Reorder columns
    column_order = [
        "address",
        "risk",
        "cluster_name",
        "cluster_category",
        "addressIdentifications_name",
        "addressIdentifications_category",
        "addressIdentifications_description",
    ] + all_categories

    df = df[column_order]

    return df


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
