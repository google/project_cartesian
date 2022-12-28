"""
Copyright 2022 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import requests
import json
import base64, requests, sys
from utilities import Utilities
from filtering_functions import FilteringFunctions
from bigquery_helper import BigqueryHelper
from datetime import datetime


params = Utilities.load_config('config.json')
MERCHANT_CENTER_MAX_RESULTS = 250
MERCHANT_CENTER_BASE_URL = "https://shoppingcontent.googleapis.com/content/v2.1/MC_ID/products/?maxResults=" + str(MERCHANT_CENTER_MAX_RESULTS)

class MerchantCenterHelper:

  def __init__(self,merchant_id: str, bq: BigqueryHelper, table: str):
    self.merchant_center_id=merchant_id
    self.table=table
    self.bq = bq

  def copy_datatransfer_table(
      self,
      destination_table_name: str,
      select_fields: [str],
      filters_dict
  ) -> None:
    """ Creates or replaces a table with data from a select statement

    Args:
      destination_table_name: The new table created with data from the filtered source table.
      fields: The comma separated fields to select. It can be * if all the fields will be selected.
      where: The where conditions on the query.
    """

    #TODO: what happens when the filter is not a string? Ex: a number
    filters_list = []
    for key in filters_dict:
      filters_list.append(key + " IN (" + ', '.join("'{0}'".format(x) for x in filters_dict[key])+ ")")

    where = ' AND '.join(filters_list)

    where = where + ' AND DATE(_PARTITIONTIME) = '
    where = where + '( DATE((SELECT MAX(_PARTITIONTIME) FROM `'+ self.bq._get_full_table_name(self.table)+'` )));'


    self.bq.create_or_replace_table_from_select(
      source_table_name= self.table,
      destination_table_name= destination_table_name,
      fields= ", ".join(select_fields),
      where= where
    )

  def normalize_fields(self, select_fields:list):
    """
    This function takes the column names of the selected fields from Merchant center and changes the composite fields that include "." in the name
    and changes it to "_" to normalize the selection.

    Args:
      select_fields: list of fields

    return flattened_fields, flattened_fields_query
    """

    bq_table = self.bq.get_bq_table(self.table)
    fields = list(filter(lambda x: x.name in select_fields, bq_table.schema))

    flatten_field = lambda x: field.name + "_" + x.name
    flatten_field_query = lambda x: field.name + "." + x.name + " AS " + field.name + "_" + x.name
    flattened_fields = []
    flattened_fields_query = []
    for field in fields:
      if field.field_type == "RECORD":
        flattened_fields = flattened_fields + list(map(flatten_field, field.fields))
        flattened_fields_query = flattened_fields_query + list(map(flatten_field_query, field.fields))
      else:
        flattened_fields.append(field.name)
        flattened_fields_query.append(field.name)


    return flattened_fields, flattened_fields_query


