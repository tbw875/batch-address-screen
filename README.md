# batch-address-screen

### Introduction

`batch-address-screen.py` is a batch process that can be run on your local machine to facilitate assessments of the Address Screen API at-scale during a pre-sales proof of value.

### Prerequisites

You must have the following to successfully run the script:

- python 3
- API Key (Best if you use your own test environment key)
- Input file with the column `['address']`
- Dependencies (`pip3 install x`)
  - pandas==1.4.3
  - getpass
  - requests
  - tqdm

### How to Use

Once you have the above Prerequisites, simply run the python script in your command line from any directory and follow the instructions.

       python batch-address-screen.py

You will need to edit the .env.example file:

- Edit with your API key
- Change filename to `.env`
- Optional: Create a `.gitignore` file with `.env` included so you don't push your API key.

The script will ask you to input:

- path-to-file for the input file (e.g. `~/Documents/folder/addresses.csv`)
- path and filename for an output (e.g. `~/Documents/folder/results.csv`)

### It's broke, yo

Let me know. tom.walsh@chainalysis.com or slack me.

Or submit an issue or pull request -- that would be much better!

### TO DO LIST

Please feel free to submit a PR if you want to work on these!

- <s>.env file for API key storage</s>
- Standardized input/output file so you don't have to manually type path/to/file
- <s>Handle parsing of Chainalysis Identifications dict.</s>
- Handle additional features of the API that are coming soon...
