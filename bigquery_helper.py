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

import datetime
import math
import json
from google.cloud import bigquery
from google.cloud import exceptions as cloud_exceptions
from typing import Optional, List
import logging
import pandas as pd
import gspread
from service_account_authenticator import Service_Account_Authenticator
from typing import Optional
import google.auth

EXPORT_LIMIT_GB = 1
GOOGLE_SHEETS_AUTH_SCOPES=["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
ENRICHED_SUFFIX="Enriched"

class BigqueryHelper:

  def __init__(self, gcp_project_id: str, dataset_name: str, bucket_name: Optional[str] = None, table_name_prefix: Optional[str] = None):
    self.gcp_project_id = gcp_project_id
    self.dataset_name = dataset_name
    self.bucket_name = bucket_name
    self.table_name_prefix = table_name_prefix

  def _get_full_table_name(self, table_name:str) -> str:
    """Generates a full table name by concatenating prefix, project, dataset, and table.

    Args:
      table_name: The name of the table.

    Returns:
      Full table name.
    """
    if self.table_name_prefix:
      return (
        self.gcp_project_id + '.' +
        self.dataset_name + '.' +
        self.table_name_prefix + "_" + table_name
      )
    return (
        self.gcp_project_id + '.' +
        self.dataset_name + '.' +
        table_name
      )


  def upload_dataframe_to_big_query(self,condensed_dataframe:pd.core.frame.DataFrame,write_disposition:str, final_joined_table_name:str):
    """
    This function uploads a dataframe to a big query table

    params:
      condensed_dataframe -> base dataframe to send to big query
      write_disposition -> OVERWRITE or APPEND
      final_joined_table_name -> Name of the table in the destination big query
    """
    #Configure Load Job to send dataframe to BQ
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    table_name = self._get_full_table_name(final_joined_table_name)
    bqclient = bigquery.Client()
    job = bqclient.load_table_from_dataframe(condensed_dataframe, table_name, job_config=job_config)  # Make an API request.
    job.result()  # Wait for the job to complete.
    table = bqclient.get_table(table_name)  # Make an API request.

    print(
      "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_name
      )
    )
    return

  def create_table(self, table_name:str, columns: list, update_if_exist: Optional[bool] = True) -> None:
    """Creates a new table with the columns provided.

    Args:
      table_name: The name of the table to create.
      columns: A list of strings with column names.
    """
    client = bigquery.Client(project=self.gcp_project_id)
    new_table_schema = []

    for column in columns:
      new_table_schema.append(bigquery.SchemaField(column, "STRING", mode="REQUIRED"))

    full_table_name = self._get_full_table_name(table_name)
    table = bigquery.Table(full_table_name, schema=new_table_schema)

    try:
      table = client.create_table(table)
    except cloud_exceptions.Conflict:
      if update_if_exist:
        table = client.update_table(table, ['schema'])
      else:
        client.delete_table(table)
        table = client.create_table(table)

  def insert_single_record(self, table_name:str, data: dict) -> None:
    """Inserts data into the table.

    Data dictionary must have keys named the same as the table's columns.

    Args:
      table_name: The name of the table to create.
      data: Dict where keys are column names and values are data to insert.
    """
    client = bigquery.Client(project=self.gcp_project_id)

    full_table_name = self._get_full_table_name(table_name)

    column_names = ', '.join(data.keys())
    values = '", "'.join(data.values())
    dml_statement = "INSERT `%s` ( %s ) VALUES ( \"%s\" )" % (full_table_name, column_names, values)

    query_job = client.query(dml_statement)
    query_job.result()

  def insert_multiple_records(self, table_name: str, data: list, header: list) -> None:
    """
    This function takes a bidimensional array and inserts sequentially to a given table

    Args:
      table_name: The name of the table to create.
      data: bidimensional array (simple rows and cols)
      header: unidimensional array with the names of the columns in order

    """
    df = pd.DataFrame(data = data, columns = header)
    job_config = bigquery.LoadJobConfig(
      # Optionally, set the write disposition. BigQuery appends loaded rows
      # to an existing table by default, but with WRITE_TRUNCATE write
      # disposition it replaces the table with the loaded data.
      write_disposition = "WRITE_APPEND"
    )

    enriched_table_name = self._get_full_table_name(table_name)

    bqclient = bigquery.Client()
    job = bqclient.load_table_from_dataframe(
      df, enriched_table_name, job_config = job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = bqclient.get_table(enriched_table_name)  # Make an API request.
    print(
      "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), enriched_table_name
      )
    )



  def delete_table(self, table_name:str) -> None:

    """Deletes a table with all of its data.

    Args:
      table_name: The name of the table to delete.
    """
    client = bigquery.Client(project=self.gcp_project_id)
    full_table_name = self._get_full_table_name(table_name)
    table = client.delete_table(full_table_name)

  def create_new_table_from_cross_join(
          self,
          tables: List[str],
          destination_table: str) -> None:
    """Creates a new table that is the product of a cross join between N existing tables.

    Args:
      tables: A list of tables to cross join.
      destination_table: Table where the result will be written.
    """
    if len(tables) == 0:
      logging.getLogger().info('There are not tables to cross join')
      return
    client = bigquery.Client(project=self.gcp_project_id)
    # Build cross join statements
    cross_join = ""
    for table in tables[1:]:  # skip first table since it goes in the select
      full_table_name = self._get_full_table_name(table)
      cross_join += f"CROSS JOIN `{full_table_name}` \n"
    dml_statement = f"""
      SELECT *
      FROM `{self._get_full_table_name(tables[0])}`
      {cross_join}
    """
    full_table_name_destination = self._get_full_table_name(destination_table)
    job_config = bigquery.QueryJobConfig(destination=full_table_name_destination)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    query_job = client.query(dml_statement, job_config=job_config)
    query_job.result()

  def read_from_table(self, table_name:str, select: Optional[str] = '*', limit: Optional[int] = None,
    offset: Optional[int] = None, where: Optional[str] = None) -> list:
    """Retrieve results from BigQuery table.

    Args:
      table_name: The name of the table to query.
      select: The list of fields to retrieve.
      where: The where conditions on the query.
      limit: The maximum number of results to return.
      offset: Offset on the query.

    Returns:
      List with table data.
    """
    client = bigquery.Client(project=self.gcp_project_id)
    full_table_name = self._get_full_table_name(table_name)

    dml_statement = (f"""
      SELECT {select}
      FROM `{full_table_name}`
      """)
    if where:
      dml_statement += f" WHERE {where}"
    if limit:
      dml_statement += f" LIMIT {limit}"
    if offset:
      dml_statement += f" OFFSET {offset}"

    query_job = client.query(dml_statement)
    result = []
    for row in query_job.result():
      result.append(row)
    return result

  def count_table_records(self, table_name:str,
    where: Optional[str] = None) -> int:
    """Count records from BigQuery table.

    Args:
      table_name: The name of the table to query.
      where: The where conditions on the query.

    Returns:
      Int, amount of records.
    """
    client = bigquery.Client(project=self.gcp_project_id)
    full_table_name = self._get_full_table_name(table_name)

    dml_statement = (f"""
      SELECT COUNT(*)
      FROM `{full_table_name}`
      """)
    if where:
      dml_statement += f" WHERE {where}"

    query_job = client.query(dml_statement)
    for row in query_job.result():
      return row[0]
    return 0

  def upload_data_to_cloud_storage(self, table_name: str) -> None:
    """Extracts data from a BigQuery table and uploads it to a Google Cloud
    Storage bucket in csv format.

    Args:
      table_name: The name of the source table.
      bucket_name: The name of the bucket where the csv file will be uploaded to.
    """
    client = bigquery.Client(project=self.gcp_project_id)
    table = client.get_table(self._get_full_table_name(table_name))
    if not self.__exceeds_limit(table.num_bytes):
      csv_file_name = f'{table_name}_extract.csv'
    else:
      # Use the wildcard operator to create multiple sharded files when table size > 1gb
      # The size of the exported files will vary.
      csv_file_name = f'{table_name}_extract_*.csv'
    destination_uri = "gs://{}/{}".format(self.bucket_name, csv_file_name)
    dataset_ref = bigquery.DatasetReference(
        self.gcp_project_id, self.dataset_name)
    table_ref = dataset_ref.table(table_name)
    # Create job to extract the data
    extract_job = client.extract_table(
        table_ref,
        destination_uri
    )  # API request
    response = extract_job.result() # Waits for job to complete.
    if not response.errors:
      logging.getLogger().info(
          "Exported {}:{}.{} to {}".format(
              self.gcp_project_id, self.dataset_name, table_name, destination_uri)
      )
    else:
      for error in response.errors:
        logging.getLogger().error(f'Error: {error["message"]} - Reason: {error["reason"]}')

  def shard_tables_by_columns(self, source_table_name: str, columns: List[str]) -> List[str]:
    """Shards a source table into different tables based on a list of columns.

    Args:
      source_table_name: The name of the table to shard.
      columns: A list of columns to use as sharding criteria.

    Returns:
      A list of sharded tables based on the provided columns.
    """
    # Group by each column and generate N tables, one per column
    x_join_table_name_list = []
    for column in columns:
      gb_destination_table_name = f'temp_values_for_{column}'
      x_join_table_name_list.append(gb_destination_table_name)
      self.create_or_replace_table_from_select(
          source_table_name, gb_destination_table_name, column, None, None, None, column)

    # Cross join the N generated tables
    x_join_destination_table_name = "_".join(x_join_table_name_list)  # TODO check for a valid name
    if len(columns) > 1:
      # To avoid existing table error when creating it
      self.create_new_table_from_cross_join(x_join_table_name_list, x_join_destination_table_name)

    # Loop through each row in the cross join table and generate a table per combination
    rows = self.read_from_table(x_join_destination_table_name)
    final_table_names = []
    for row in rows:
      table_names = []
      where_conditions = []
      for idx, col in enumerate(row):
        where_conditions.append(f"{columns[idx]}='{col}'")
        table_names.append(columns[idx])
        table_names.append(col)
      where_clause = " AND ".join(where_conditions)
      final_table_name = "_".join(table_names)  # TODO check for a valid name
      final_table_names.append(final_table_name)
      self.create_or_replace_table_from_select(
          x_join_destination_table_name, final_table_name, '*', None, None, where_clause, None)

    return final_table_names


  def create_or_replace_table_from_select(self, source_table_name: str, destination_table_name: str,
    fields: Optional[str] = None, limit: Optional[int] = None, offset: Optional[int] = None,
    where: Optional[str] = None, group_by: Optional[str] = None) -> None:
    """ Creates or replaces a table with data from a select statement

    Args:
      source_table_name: The name of the table to get the data from.
      destination_table_name: The new table created with data from the filtered source table.
      fields: The comma separated fields to select. It can be * if all the fields will be selected.
      limit: The maximum number of results to return.
      offset: Offset on the query.
      where: The where conditions on the query.
      group_by: Columns to group by the data.
    """
    client = bigquery.Client(project=self.gcp_project_id)
    full_source_table_name = self._get_full_table_name(source_table_name)
    full_destination_table_name = self._get_full_table_name(destination_table_name)
    dml_statement = (f"""
      CREATE OR REPLACE TABLE {full_destination_table_name}
      AS
      SELECT {fields}
      FROM `{full_source_table_name}`
      """)
    if where:
      dml_statement += f" WHERE {where}"
    if group_by:
      dml_statement += f" GROUP BY {group_by}"
    if limit:
      dml_statement += f" LIMIT {limit}"
    if offset:
      dml_statement += f" OFFSET {offset}"

    query_job = client.query(dml_statement)
    query_job.result()

  def flatten_list(self,_2d_list: list) -> list:
    """
    Converts an array of arrays into a single dimensional array
    Args:
    _2d_list: array of arrays

    return
    single dimension array
    """

    flat_list = []
    # Iterate through the outer list
    for element in _2d_list:
        if type(element) is list:
            # If the element is of type list, iterate through the sublist
            for item in element:
                flat_list.append(item)
        else:
            flat_list.append(element)
    return flat_list


  def condense_rows_from_table_in_memory(self, source_table_name: str, destination_table_name: str,
    amount_of_rows_to_condense: int, columns: List[str]) -> None:
    """
    Creates a table that condenses multiple rows into a single one.
    Output from this function is a new table that includes data from
    multiple rows in the source table, having all columns in the source
    table with a number indicating the index from the n rows that were
    condensed. This function processes locally and links products toguether randomly
    Args:
      source_table_name: The name of the table to get the data from.
      destination_table_name: The new table created with data from the filtered source table.
      amount_of_rows_to_condense: The amount of rows that will become a single row in the 
        destination table.
      columns: List of columns in the source table.
    """

    original_table=self.get_big_query_table_as_df(source_table_name)
    sample_row_len=0
    condensed_array=[]
    while len(original_table) >= amount_of_rows_to_condense:
      condensed_row=[]
      for i in range(amount_of_rows_to_condense):
        #sacamos random sample y borramos del dataframe
        random_sample=original_table.sample()
        sample_index=random_sample.index[0]
        original_table = original_table.drop(sample_index)
        random_sample_row=None
        for index,row in random_sample.iterrows():
          random_sample_row=row
          if sample_row_len==0:
            sample_row_len=len(random_sample_row)
        for element in random_sample_row:
          condensed_row.append(element)
      condensed_array.append(self.flatten_list(condensed_row))
    final_columns=columns*amount_of_rows_to_condense
    temp=[]
    index_col=1
    index_ctl=0
    for col in final_columns:
      temp.append(col+"_"+str(index_col))
      index_ctl+=1
      if index_ctl==sample_row_len:
        index_col+=1
        index_ctl=0
    final_columns=temp
    df = pd.DataFrame(condensed_array, columns = final_columns)
    self.upload_dataframe_to_big_query(df,"WRITE_TRUNCATE", destination_table_name)
    return


  def condense_rows_from_table_in_bigquery(self, source_table_name: str, destination_table_name: str,
    amount_of_rows_to_condense: int, columns: List[str]) -> None:
    """Creates a table that condenses multiple rows into a single one.
    Output from this function is a new table that includes data from
    multiple rows in the source table, having all columns in the source
    table with a number indicating the index from the n rows that were
    condensed.
    Args:
      source_table_name: The name of the table to get the data from.
      destination_table_name: The new table created with data from the filtered source table.
      amount_of_rows_to_condense: The amount of rows that will become a single row in the
        destination table.
      columns: List of columns in the source table.
    Example:
      Source table:
        col_a   col_b   col_c
          1       2       3
          4       5       6
          7       8       9
          10      11      12
      Calling this function sending amount_of_rows_to_condense = 2,
      columns = [col_a, col_b, col_c]
      Destination table:
        col_a_1 col_b_1 col_c_1 col_a_2 col_b_2 col_c_2
           1       2       3       7       8       9
           4       5       6       10      11      12
    """
    client = bigquery.Client(project=self.gcp_project_id)
    total_rows = self.count_table_records(source_table_name)
    new_amount_of_rows = math.ceil(total_rows / amount_of_rows_to_condense)
    full_source_table_name = self._get_full_table_name(source_table_name)
    full_destination_table_name = self._get_full_table_name(destination_table_name)
    group_number = 1
    columns_renamed = ",".join(self._rename_columns(group_number, columns))
    dml_statement = (f"""
      CREATE OR REPLACE TABLE `{full_destination_table_name}`
      AS
      WITH group{group_number} AS (
        SELECT {columns_renamed},
        ROW_NUMBER() OVER(ORDER BY 1 ASC) AS row
        FROM `{full_source_table_name}`
        ORDER BY row ASC
        LIMIT {new_amount_of_rows} OFFSET 0
      )
      """)
    # SQL Statement that creates a new table using the renamed column names
    # (that include the group number) using a limit and an offset to consider
    # different rows and join them together. The row_number (counter) is used
    # as the join key, joining the first row of each group, the second row of
    # each group and so on.
    dml_joins = ""
    while group_number < amount_of_rows_to_condense:
      group_number += 1
      offset = new_amount_of_rows * (group_number - 1)
      columns_renamed = ",".join(self._rename_columns(group_number, columns))
      dml_statement += f"""
        , group{group_number} AS (
          SELECT {columns_renamed},
          ROW_NUMBER() OVER(ORDER BY 1 ASC) - {offset} as row
          FROM `{full_source_table_name}`
          ORDER BY row ASC
          LIMIT {new_amount_of_rows} OFFSET {offset}
        )
      """
      dml_joins += f"""
        LEFT JOIN group{group_number}
        USING (row)
      """
      # Within the while statement, joins are added to the SQL statement with
      # offsets and limits until all groups are considered.
    dml_statement += f"""
        SELECT * EXCEPT (row)
        FROM group1
      """ + dml_joins
    query_job = client.query(dml_statement)
    query_job.result()

  def _rename_columns(self, group_number:int, columns: List[str]):
    """Returns an array of columns with aliases including the group number.

    Starting from a list of columns (strings), returns another list of strings
    with column names with aliases to be used in a select statement.
    [a, b, c] becomes [a AS a_group_number, b AS b_group_number, c AS c_group_number]

    Args:
      group_number: Integer to concatenate with the column name to form the new alias
        for the column.
      columns: List of columns to rename.
    """
    columns_renamed = []

    for column in columns:
      columns_renamed.append(f"{column} AS {column}_{group_number}")

    return columns_renamed

  def __exceeds_limit(self, bytes) -> bool:
    """Checks if a table size exceeds the 1 gb limit for data export jobs in BQ.

    Args:
      bytes: The number of bytes.

    Returns
      True if the size exceeds the limit, False otherwise
    """
    gb = float(bytes) / (1024 ** 3)
    return gb > EXPORT_LIMIT_GB




  def get_big_query_table_as_df(self,table_name : str) -> pd.core.frame.DataFrame:
    """
    Selects all from a specified table (table name) only the name is required, the full table name in cloud will be completed within the function
    After getting the result it fits it into a pandas dataframe and returns it

    Args:
      string: table name

    Returns
      dataframe with the table contents
    """

    bqclient = bigquery.Client()
    full_source_table_name = self._get_full_table_name(table_name)

    query_string = "SELECT * FROM "+ full_source_table_name

    dataframe = (
    bqclient.query(query_string)
    .result()
    .to_dataframe(
                 # Optionally, explicitly request to use the BigQuery Storage API. As of
                 # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
                 # API is used by default.
                 create_bqstorage_client=True,
                 )
                )
    return dataframe

  def clear_table_google_sheets(self,google_sheet_name:str):
    """
    Clear google sheet to avoid issues
    """
    credentials, project_id = google.auth.default(
        scopes=GOOGLE_SHEETS_AUTH_SCOPES
    )
    client = gspread.authorize(credentials)

    spreadsheet=client.open(google_sheet_name)
    worksheet = spreadsheet.get_worksheet(0)
    worksheet.batch_clear(["A1:AZ100000"])
    

  def send_table_to_google_sheets(self,table_name:str,output_google_sheet_name:str,share_with: str) -> str:
    """
    This function authenticates with Google sheets and sends a table to the sheet

    params:

    table_name -> Name of the table in BQ
    google_sheets_prefix -> The name of the Google sheet to be created or edited to put the info in

    share_with -> email of the person that is going to be able to see the sheet


    Usage>

    from bigquery_helper import BigqueryHelper
    #Create Bigquery Helper
    bq_helper=BigqueryHelper("project-id","ds-name","bucket")
    bq_helper.send_table_to_google_sheets("tab-name","example","atomas@google.com")
    """

    #Adhoc function to map the BQ return object to a bidimensional matrix ready for sheets
    def get_info(row):
      return list(row)
    #Authenticate with google sheets
    
    credentials, project_id = google.auth.default(
        scopes=GOOGLE_SHEETS_AUTH_SCOPES
    )
    client = gspread.authorize(credentials)
    #get data from table and send to google sheet
    table_data=self.read_from_table(table_name)
    data=list(map(get_info,table_data))
    df = pd.DataFrame(data, columns = table_data[0].keys())
    sheets_file=df.to_csv(index=False)
    try:
      spreadsheet=client.open(output_google_sheet_name)
    except gspread.exceptions.SpreadsheetNotFound :
      print("This sheet doesn't exist. Creating one...")
      spreadsheet=client.create(output_google_sheet_name)
    client.import_csv(spreadsheet.id,data=sheets_file.encode("utf-8"))
    spreadsheet.share(share_with, perm_type='user', role='writer')

  def get_bq_table(self, table_name):
    full_table_name = self._get_full_table_name(table_name)

    client = bigquery.Client(project=self.gcp_project_id)
    table = client.get_table(full_table_name)

    return table

