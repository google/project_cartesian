# Copyright 2022 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import json
from flask import Flask, request
from bigquery_helper import BigqueryHelper
from utilities import Utilities
from merchant_center_helper import MerchantCenterHelper
from service_account_authenticator import Service_Account_Authenticator
import pandas as pd
from google.cloud import bigquery
import gspread
import google.auth


CONDENSED_SUFFIX="Condensed"
STUDIO_ID="id"
STUDIO_ACTIVE="active"
STUDIO_DEFAULT="default"
STUDIO_REPORTING_ID="reporting_id"
STRING_TRUE="TRUE"
STRING_FALSE="FALSE"
WRITE_DISPOSITION_FINAL_TABLE="WRITE_TRUNCATE" #Could be WRITE_APPEND
ENRICHED_SUFFIX="Enriched"
PRODUCTS_FROM_MC = "productsFromMC"
GOOGLE_SHEETS_AUTH_SCOPES=["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]


app = Flask(__name__)
params = Utilities.load_config('config.json')
merchant_center_fields=params["mc_fields"]


def _create_table_for_mc_products(bq:BigqueryHelper, products_from_mc_in_array):
    """
    Takes products from array and inserts them in product table

    table_name = Name taken from class variable defined at the beggining
    products_from_mc_in_array = Products from MC filtered by attributes defined in config file

    """
    bq.create_table(table_name=PRODUCTS_FROM_MC, columns=merchant_center_fields, update_if_exist=False)
    bq.insert_multiple_records(PRODUCTS_FROM_MC, products_from_mc_in_array, merchant_center_fields)


def _create_options_tables(bq:BigqueryHelper)-> list:
    """
    Reads config file to retrieve options for complementary tables, iterates through them
    and store values in their own table

    Returns a list with the table names created in this method, as well as the header list

    Example input:
    "additional_columns":{
      "option1":["value1","value2"],
      "option2":["value3","value4"]
    }
    """
    options = []
    options_table_header_list = []
    for key,values in params["additional_columns"].items():
        table_name=key+"Table"
        bq.create_table(table_name=table_name, columns=[key])
        bq.insert_multiple_records(table_name,map(lambda x: [x], values),[key])
        options.append(table_name)
        options_table_header_list.append(key)

    return options,options_table_header_list


def _get_df_reporting_ids(base_fields: list, df: pd.core.frame.DataFrame) -> list:
    """
    Receives a dataframe and list of headers in the DF of the columns that are going to be taken into account to create the reporting_id
    The reporting id will be a concatenation of these fields divided by "_"

    params:
      base_fields: list of strings with the names of the fields that are going to be used to generate the composed reporting id
      df: base dataframe with the studio feed

    return:
    list of reporting ids to add to the datadrame

    """

    reporting_ids = []
    for index, row in df.iterrows():
        fields = []
        for element in base_fields:
            fields.append(str(row[element]))

        reporting_ids.append("_".join(fields))
    return reporting_ids


def _add_studio_required_columns(condensed_dataframe: pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
    """
    Adding Google Studio required cols.

    For the resulting feed to be valid for Google Studio, it needs an ID field, an "active"
    boolean field set to true, and a reporting_id to be used for reporting in Campaign manager.
    This function adds those columns.

    params:
        condensed_dataframe: Pandas dataframe with all the product data.

    returns:
        Pandas dataframe with the same data, plus the additional studio columns.
    """

    condensed_dataframe[STUDIO_ID]=list(range(1,len(condensed_dataframe)+1))
    condensed_dataframe[STUDIO_ACTIVE]=[STRING_TRUE]*len(condensed_dataframe)
    condensed_dataframe[STUDIO_DEFAULT]=[STRING_FALSE]*len(condensed_dataframe)
    reporting_id_cols = ["id"]

    #If we condensed several items in a row, we need to create a reporting id with data from all
    #condensed items.
    if params["amount_of_rows_to_condense"] and params["amount_of_rows_to_condense"] > 1:
        for i in range(params["amount_of_rows_to_condense"]):
            reporting_id_cols.append(params["reporting_id_column"] + '_' + str(i+1))
    elif params["amount_of_rows_to_condense"] and params["amount_of_rows_to_condense"] == 1:
        reporting_id_cols.append(params["reporting_id_column"])

    condensed_dataframe[STUDIO_REPORTING_ID] = _get_df_reporting_ids(reporting_id_cols, condensed_dataframe)

    return condensed_dataframe


def main_cartesian():
    bq = BigqueryHelper(
        gcp_project_id=str(params["gcp_project_id"]),
        dataset_name=str(params["bigquery_dataset"]),
        bucket_name=str(params["bucket_name"]),
        table_name_prefix=str(params["table_name_prefix"])
    )

    mc = MerchantCenterHelper(
        merchant_id=str(params["mc_id"]),
        bq=bq,
        table=str(params["mc_datatransfer_table"])
    )


    normalized_fields, normalized_fields_query = mc.normalize_fields(merchant_center_fields)
    #Makes a copy of the MC table, but only selected columns and rowtable, but only selected columns and rows
    print("normalized_fields")
    print(normalized_fields)
    print("normalized_fields_query")
    print(normalized_fields_query)
    mc.copy_datatransfer_table(PRODUCTS_FROM_MC, normalized_fields_query, params["attribute_filters"])


    #Creates secondary table with extra options needed to merge into products
    options_tables,options_table_header_list = _create_options_tables(bq)

    #Cross join table products with extra options
    final_joined_table = _cross_join_tables(options_tables, bq)

    #Appends optional headers into MC header list to be used for condensed table
    for header in options_table_header_list:
        normalized_fields.append(header)

    #Condense tables to get a final table containing merged products with options
    if params["amount_of_rows_to_condense"] and params["amount_of_rows_to_condense"] > 1:
        condensed_table_name = final_joined_table + CONDENSED_SUFFIX
        bq.condense_rows_from_table_in_memory(final_joined_table, condensed_table_name, params["amount_of_rows_to_condense"], columns = normalized_fields)
        final_joined_table = condensed_table_name

    dataframe = bq.get_big_query_table_as_df(final_joined_table)
    dataframe = _add_studio_required_columns(dataframe)

    final_table_with_studio_data = final_joined_table + ENRICHED_SUFFIX

    bq.upload_dataframe_to_big_query(dataframe, WRITE_DISPOSITION_FINAL_TABLE, final_table_with_studio_data)

    output_google_sheet_name=str(params["output_google_sheet_name"])
    administrator_email=str(params["administrator_email"])
    #Clear current Google sheet
    bq.clear_table_google_sheets(output_google_sheet_name)
    #Write google sheets
    bq.send_table_to_google_sheets(final_table_with_studio_data, output_google_sheet_name, administrator_email)
    return

def _transform_config_to_json(list_of_lists: list)-> dict:
  """
Takes the configuration sheet as a list of lists and transforms it to a dictionary
params:
        list_of_lists: Config as list of lists.

    returns:
        configuration json as a dictionary

  """
  raw_list=list_of_lists[1:len(list_of_lists)]
  final_json={}
  for pair in raw_list:
    try:
      final_json[pair[0]]=json.loads(pair[1])
    except:
      final_json[pair[0]]=pair[1]
  return final_json


def _load_config(input_google_sheet_name:str)-> str:
    """
    Takes a google sheets name, transforms to json and updates configuration file and current variables
        params:
            input_google_sheet_name: String with the google sheets name.

        returns:
            New configuration json

    """

    global params
    global merchant_center_fields
    credentials, project_id = google.auth.default(
        scopes=GOOGLE_SHEETS_AUTH_SCOPES
    )
    client = gspread.authorize(credentials)
    try:
      spreadsheet=client.open(input_google_sheet_name)
    except gspread.exceptions.SpreadsheetNotFound :
      print("Configuration sheet does not exist!...")
      return "Configuration sheet does not exist!... Not updated"
    worksheet = spreadsheet.get_worksheet(0)
    list_of_lists = worksheet.get_all_values()
    config_json=_transform_config_to_json(list_of_lists)
    params=config_json
    merchant_center_fields=params["mc_fields"]
    f = open("config.json", "w")
    json.dump(config_json, f)
    f.close()
    a_file = open("config.json","r")    
    Lines = a_file.readlines()
    for line in Lines:
      print(line)
    a_file.close()
    return line



@app.route("/execute")
def deploy():
    main_cartesian()
    return "Main cartesian executed successfully!\n"


@app.route("/test")
def test_deploy():
    return "Project Cartesian deployed successfully!\n"

@app.route("/updateConfig")
def configure():
    """
    Reads a google sheet by name with the configuration translates it to json writes it to file and global variables
    Returns the new configuration as a json.
    """
    try:
      sheet=request.args.get("sheet_name")
      line=_load_config(sheet)
      return line
    except Exception as e:
      print(e)
      return "Loading Unsuccesful!" 



def _create_additional_columns_tables(bq:BigqueryHelper) -> list:
    """
    Reads config file to retrieve options for complementary tables, iterates through them
    and store values in their own table

    Args:
        bq : Instance of BigQueryHelper to help with auxiliar operations

    Return:
        Returns a list with the table names created in this method

    Example input in config file:
    "options_table":{
      "option1":["value1","value2"],
      "option2":["value3","value4"]
    }
    """
    additional_columns = []
    for key,values in params["additional_columns"].items():
        table_name=key+"Table"
        bq.create_table(table_name=table_name, columns=[key])
        bq.insert_multiple_records(table_name,map(lambda x: [x], values),[key])
        additional_columns.append(table_name)
        additional_columns_header_list.append(key) #Stored in class variable

    return options

def _cross_join_tables(additional_columns_tables:list, bq:BigqueryHelper) -> str:
    """This method takes a list of the tables created as options and joins them
        with the product list
    Args:
      additional_columns_tables : List of tables names created for the options to add to product table
      bq : Instance of BigQueryHelper for auxiliary operations
    Return:
      Name of table where products where merged with options specified in config file
    """
    current_table = PRODUCTS_FROM_MC
    for column in additional_columns_tables:
        cross_table = current_table+column
        bq.create_new_table_from_cross_join(tables=[current_table, column], destination_table=cross_table)
        current_table = cross_table

    return current_table


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
