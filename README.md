## README Project Cartesian

## Overview

Project Cartesian is a tool that builds a feed for Dynamic Creatives on Studio, based on product data already in the Merchant Center. This tool can be used to manage an exponentially growing feed, by adding columns with different values, partitioning the data based on different dimensions, or joining multiple products into a single ad. The end result is a Spreadsheet that can be connected with Studio in order to easily run campaigns using this shopping data, with one or multiple products per ad. Google doesn't officially license this tool and it should be used on the user’s own responsibility.

## Set up

Create a GCP Project or use an existing one.

Download the git repo.

Open Cloud Shell

git clone https://github.com/googlestaging/project_cartesian

cd into the newly-downloaded folder.

Set-up your config variables

Open file config.json in an editor (either the Cloud shell editor, or any of the command-line editors such as vim or nano)

Fill in required parameters, see section on config variables for more information.

Run the setup script

./setup.sh

During the setup you may be asked to confirm certain operations by typing “Y”.

Enable Merchant Center Data Transfer

Verify that you have completed all actions required to enable the BigQuery Data Transfer Service.

Create a BigQuery dataset to store the Google Merchant Center data.

Configure the required permissions.

Set up the Data Transfer 

Download the Service Account authentication JSON and configure the project to use it

Open the Cloud Console, select your Cartesian Project

Open the Service Accounts IAM page (selecting IAM in the left side menu or by searching “Service Accounts” in the search bar).

Find the row with the service account, click on it.

Select the “Keys” top menu.

If no keys exist, click on Add Key > Create new key > JSON.

Download the JSON file with the keys

Upload the JSON to the directory where Cartesian project was downloaded (one way of doing this is opening the Cloud Shell Editor > Right click on the folder with the code > upload file)

Update the config.json adding the name of the JSON file in the “service_account_credentials” variable.

Create a Google Sheet for the Studio Feed.

Name the sheet with simple characters, no spaces, numbers or symbols (it must match the value you put in the config.json for field output_google_sheet_name).

Share this document with “editor” role with the service account previously created (if you left the defaults, this service account will be “cartesian-service-account@PROJECT-ID.iam.gserviceaccount.com)

(Optional) Create the Google Sheets proxy for the config.json modified in step 4.

Create a copy of this document.

Rename the sheet with simple characters, no spaces, numbers or symbols. 

Share it with the service account with the “editor”role.

Copy the values you’ve input in the config.json in step 4.


## Execution

As part of the setup script, Cartesian can create a cloud scheduler that automatically runs this process either daily, weekly or monthly (depending on the configured parameters), so no execution steps are required unless the value “none” is entered in the configuration parameter “auto_run_schedule”. As part of the execution, the most recent Merchant Center data is used to create the feed and write it into the Google Sheet configured for the output. Subsequent executions will update the google sheet.

You can manually call the Cloud Run endpoint to execute the processes. You can obtain the Cloud Run URL from the Cloud Console (type Cloud Run into the search bar). From any terminal, you can call this command for it to execute using the credentials of the person who’s executing the command:
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" "https://CLOUD_RUN_URL/execute"

Optionally, you can set up another Cloud Scheduler to update the configuration file. If you often change configuration parameters, make sure you create a copy of the configuration Google Sheet (in setup step 9). Log into the Cloud Console and configure a second Cloud Scheduler to run before the one that’s already configured, using the following URL: https://CLOUD_RUN_ENDPOINT/updateConfig?sheet_name=NAME_OF_THE_CONFIG_GOOGLE_SHEET. Make sure you replace the Cloud Run endpoint and the name of the Configuration Sheet, from the setup step 9.

