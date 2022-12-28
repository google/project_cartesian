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
import base64, requests, sys
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utilities import Utilities



params = Utilities.load_config('config.json')
json_name=str(params["service_account_credentials_path"])



class Service_Account_Authenticator:

  def __init__(self,scope:list):
    
    self.credentials_json=json_name
    self.scope=scope
    self.service_account_credentials=self.authenticate()

  def authenticate(self):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(self.credentials_json, self.scope)
    return credentials
  def get_service_account_credentials(self):
    return self.service_account_credentials

