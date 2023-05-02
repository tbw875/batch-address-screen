# batch-address-screen

### Introduction

`batch-address-screen.py` is a batch process that can be run on your local machine to facilitate assessments of the Address Screen API at-scale during a pre-sales proof of value.

### Prerequisites

You must have the following to successfully run the script:

- python 3 (Ideally 3.9.15)
- API Key (Best if you use your own test environment key)
- Input file with the column `address`
- Dependencies (`pip3 install x` or use the requirements.txt file)
  - pandas==1.4.3
  - requests
  - tqdm
  - python-dotenv

### How to Use

Once you have the above Prerequisites, simply run the python script in your command line from any directory and follow the instructions.

       python batch-address-screen.py your_address_file.csv

You will need to edit the .env.example file:

- Edit with your API key
- Change filename to `.env`
- Optional: Create a `.gitignore` file with `.env` included so you don't push your API key.

### It's broke, yo

Let me know. tom.walsh@chainalysis.com or slack me.

Or submit an issue or pull request -- that would be much better!

### TO DO LIST

Please feel free to submit a PR if you want to work on these!

- <s>.env file for API key storage</s>
- <s>Standardized input/output file so you don't have to manually type path/to/file</s>
- <s>Handle parsing of Chainalysis Identifications dict.</s>
- <s>Handle additional features of the API that are coming soon...</s>
- <s>Refactor as a set of functions that are called so debugging is easier.</s>
- Introduce upload interface and Lambda function backend
