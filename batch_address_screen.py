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
    """
    Process a DataFrame of addresses through the Chainalysis API.

    Args:
        df (pandas.DataFrame): DataFrame containing addresses to process.
        headers (dict): Headers for API requests.

    Returns:
        list: List of JSON responses from the API.

    The function iterates over each address in the DataFrame, sends a POST request to register the address,
    and then sends a GET request to retrieve the response. If there is an error with either request (status code 400 or 500),
    a warning is logged and the processing continues to the next address.

    The responses from successful requests are stored in a list and returned at the end of the function.

    Note: tqdm is used to provide progress bar functionality during the iteration.
    """
    print("Processing addresses through API ...")
    responses = []

    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        # note: `index` is necessary for tqdm
        address = row["address"]

        register_url = "https://api.chainalysis.com/api/risk/v2/entities"
        new_payload = json.dumps({"address": address})
        request = requests.request(
            "POST", register_url, headers=headers, data=new_payload, timeout=60
        )
        if request.status_code in [400, 500]:
            logging.warning(
                "Error %s: Something went wrong with the API request (POST) for address %s.",
                request.status_code,
                address,
            )

            print(
                f"Error {request.status_code}: Something went wrong with the API request (POST) for address {address}."
            )
            continue

        fetch_url = f"https://api.chainalysis.com/api/risk/v2/entities/{address}"
        response = requests.request("GET", fetch_url, headers=headers, timeout=60)
        if request.status_code in [400, 500]:
            logging.warning(
                "Error %s: Something went wrong with the API request (GET) for address %s.",
                response.status_code,
                address,
            )

            print(
                f"Error {response.status_code}: Something went wrong with the API request (GET) for address {address}."
            )
            continue
        else:
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
    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(responses, f)


def process_responses(responses):
    """
    Process the responses from the Chainalysis API.

    Args:
        responses (list): List of JSON responses from the API.

    Returns:
        pandas.DataFrame: Processed DataFrame containing the parsed responses.

    The function takes a list of JSON responses and performs the following steps:

    1. Flattens nested dictionaries in the JSON responses.
    2. Creates a DataFrame from the flattened JSON.
    3. Creates a dictionary to store exposure values for each address.
    4. Pivots the exposures DataFrame to have exposure categories as columns.
    5. Populates the exposure categories in the DataFrame.
    6. Drops the "exposures" column from the DataFrame.
    7. Reorders the columns in a specified order.
    8. Ensures that required columns are always present in the DataFrame.

    The processed DataFrame is then returned as the result of the function.
    """
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

    # Ensure required columns are always present
    for col in column_order:
        if col not in df.columns:
            df[col] = None

    df = df[column_order]

    return df


def save_output_csv(merged_df):
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
    """
    Main function to execute the batch address screening process.

    The function performs the following steps:

    1. Sets up logging configuration.
    2. Loads the API key from environment variables.
    3. Gets the headers for the API request.
    4. Parses the command line arguments to get the CSV file path.
    5. Reads the input CSV file into a DataFrame.
    6. Calls the API to process the addresses and saves the raw JSON responses to disk.
    7. Processes the API responses to create a DataFrame with parsed data.
    8. Saves the processed data to an output CSV file.

    The function serves as the entry point to execute the batch address screening process.
    """
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
