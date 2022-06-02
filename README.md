# batch-address-screen

### Introduction
`batch-address-screen.py` is a batch process that can be run on your local machine to facilitate assessments of the Address Screen API at-scale during a pre-sales proof of value.

### Prerequisites
You must have the following to successfully run the script:
* python 3
* API Key (Best if you use your own test environment key)
* Input file with the columns `['user_id','asset','address']`
* Dependencies (`pip3 install x`)
  * pandas
  * getpass
  * requests
  * tqdm

### How to Use
Once you have the above Prerequisites, simply run the python script in your command line from any directory and follow the instructions.

       python batch-address-screen.py

The script will ask you to input:

  * API key to be used for the API calls
  * path-to-file for the input file (e.g. `~/Documents/folder/addresses.csv`)
  * path and filename for an output (e.g. `~/Documents/folder/results.csv`)

### It's broke, yo

Let me know. tom.walsh@chainalysis.com or slack me.

Or submit an issue or pull request -- that would be much better!
