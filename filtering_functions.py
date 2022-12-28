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
class FilteringFunctions:
  """
  The purpose of this class is to define special logic to be used as a filter when obtaining the merchant center feed. For example function "custom_label_one_is_237" fecined below
  will return true if the field from merchant center "custom_label_one" in a certain product is equal to the string 237.
  All functions in this class must receive only one parameter other than "self". and must only return true or false.

  The functions specified here are only intended to be used with the "transform_json_to_table_customized" function defined in this class and with json objects obtained from merchant center
  """

  def __init__(self):
    return

  def content_is_spanish(self,content):
    """
    This function will return true only if the field to which it is applied is exacly "es". Meant to be aplied to the language field in the merchant center json response
    """
    if content=='es':
      return True
    else:
      return False

  def return_true(self,content):
    return True
    
  def content_is_237(self,content):
    """
    This function will return true only if the field to which it is applied is exacly "237". Meant to be aplied to the custom label field in the merchant center json response
    """
    if content=='237':
      return True
    else:
      return False

  def transform_json_to_table_customized(self, json_list: list, fields: list, filters:dict) -> list:

    """
    This function takes the list of jsons and filters the fields required for the final feed (bidimensional matrix)

    Args:
      json_list : list of json objects representing articles in the merchant center 
      fields: list of objects with keys representing the name of the field of interent from MC  and as a value a string with the access route to the field
      filters: A dictionary where the keys are field names from merchant center and the values are functions that return true or false and with a single parameter
      these functions should return true if the field should be taken and false if the field should not be taken

      #fields  --> [{"title":"['title']"},{"customAttrName":"['customAttributes'][0]['name']"}]
      #filters --> {"title":function with one parameter that returns true or false, "customAttrName": other function with one parameter that returns true or false}


      Example usage

      downloader=MerchantCenterHelper()
      functions=FilteringFunctions()
      fields=[{"customLabel1":"['customLabel1']"},{"offerId":"['offerId']"},{"title":"['title']"},{"brand":"['brand']"}]
      filters={"customLabel1":functions.content_is_237, "offerId":functions.return_true, "title":functions.return_true, "brand":functions.return_true}
      final=functions.transform_json_to_table_customized(downloader.raw_list,fields,filters)
      
    Return:
     Bidimensional array with the merchant center field with the filters applied

    """

    final_matrix=[];
    for i in range(len(json_list)):
      filter_control=[]
      additive_row=[]
      for field in fields:
        first_key = list(field.keys())[0]
        filter_function=filters[first_key]
        parameter_value=None
        d={"json_list":json_list, "i":i}
        exec("x=json_list[i]"+field[first_key],d)
        parameter_value=d['x']
        additive_row.append(parameter_value)
        veredict=filters[first_key](parameter_value)
        filter_control.append(veredict)
        if not veredict:
          break
      if(all(filter_control)):
        final_matrix.append(additive_row)
      else:
        continue
    self.final_matrix=final_matrix
    return final_matrix
