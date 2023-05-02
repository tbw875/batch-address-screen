import unittest
import pandas as pd
import os
from batch_address_screen import (
    load_environment_variables,
    read_input_file,
    setup_logging,
    get_headers,
    process_responses,
    save_output_csv,
)


sample_responses = [
    {
        "address": "bc1pkfeeh92s89gcrr0gr92cku7kkxyy4lg34c8wkfjrp4rsxyc4w4vsffy4eu",
        "risk": 1,
        "cluster": {"name": "Sample Cluster", "category": "exchange"},
        "exposures": [],
        "addressIdentifications": [],
    },
    {
        "address": "32MbP3TCF9crsLNLjU5jGLDngHjZuHtYv1",
        "risk": 1,
        "cluster": {"name": "Sample Cluster 2", "category": "exchange"},
        "exposures": [{"category": "exchange", "value": 0.5}],
        "addressIdentifications": [
            {"name": "Sample ID", "category": "exchange", "description": "Sample Desc"}
        ],
    },
]


class TestScript(unittest.TestCase):
    def test_process_responses(self):
        result = process_responses(sample_responses)

        self.assertIsInstance(result, pd.DataFrame)
        expected_columns = [
            "address",
            "risk",
            "cluster.name",
            "cluster.category",
            "addressId.name",
            "addressId.category",
            "addressId.description",
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

        self.assertEqual(list(result.columns), expected_columns)

    def test_load_environment_variables(self):
        api_key = load_environment_variables()
        self.assertIsNotNone(api_key)

    def test_read_input_file(self):
        csv_path = "sample_addresses.csv"
        df = read_input_file(csv_path)
        self.assertIsNotNone(df)
        self.assertTrue("address" in df.columns)

    def test_setup_logging(self):
        setup_logging()
        self.assertTrue(os.path.exists("logs"))
        self.assertTrue(os.path.isfile("logs/progress.log"))

    def test_get_headers(self):
        api_key = "dummy_api_key"
        headers = get_headers(api_key)
        self.assertIsNotNone(headers)
        self.assertEqual(headers["token"], api_key)
        self.assertEqual(headers["Content-type"], "application/json")

    def test_save_output_csv(self):
        sample_df = pd.DataFrame(
            {"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]}, columns=["A", "B", "C"]
        )
        save_output_csv(sample_df)
        self.assertTrue(os.path.exists("results"))
        self.assertTrue(
            os.path.isfile("results/Chainalysis_AddressScreeningAPI_Results.csv")
        )


if __name__ == "__main__":
    unittest.main()
