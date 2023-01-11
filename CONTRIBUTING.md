
# Contributing

## Set up

- Create a GCP Project or use an existing one.

- Download the git repo.

- Open Cloud Shell
  ```bash
  git clone https://github.com/googlestaging/project_cartesian
  ```

- cd into the newly-downloaded folder.

- Set-up your config variables

- Open file `config.json` in an editor (either the Cloud shell editor, or any of the command-line editors such as vim or nano)

- Fill in required parameters, see section on config variables for more information.

- Run the setup script
  ```bash
  ./setup.sh
  ```
  During the setup you may be asked to confirm certain operations by typing `Y`.

- Enable Merchant Center Data Transfer

- Verify that you have completed all actions required to enable the `BigQuery Data Transfer Service`.

- Create a BigQuery dataset to store the Google Merchant Center data.

- Configure the required permissions.

- Set up the Data Transfer 

- Download the Service Account authentication JSON and configure the project to use it

- Open the Cloud Console, select your Cartesian Project

- Open the `Service Accounts IAM page` (selecting IAM in the left side menu or by searching “Service Accounts” in the search bar).

- Find the row with the service account, click on it.

- Select the `Keys` top menu. If no keys exist, click on `Add Key` > `Create new key` > `JSON`.

- Download the JSON file with the keys

- Upload the JSON to the directory where Cartesian project was downloaded (one way of doing this is opening the Cloud Shell Editor > Right click on the folder with the code > upload file)

- Update the `config.json` adding the name of the JSON file in the `service_account_credentials` variable.

- Create a Google Sheet for the Studio Feed. Name the sheet with simple characters, no spaces, numbers or symbols (it must match the value you put in the `config.json` for field `output_google_sheet_name`).

- Share this document with *editor* role with the service account previously created (if you left the defaults, this service account will be `cartesian-service-account@PROJECT-ID.iam.gserviceaccount.com`)


> For additional details check: [Google Docs](https://docs.google.com/document/d/1miPdoPKnUXR-Q-Clmf5nY7GAkOAlKhXDpWC2MvUQ1hg/edit?resourcekey=0-bSG5TEQZsetX6KH4zVG29w#heading=h.yxorsnqgsslm)
