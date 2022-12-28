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
import json

class Utilities:

  def load_config(config_file_name):
    """Loads the configuration data from the given path.

    Args:
      config_file_name: The name of the configuration file to load.

    Returns:
      The contents of the configuration file as a JSON object.
    """
    config_file_path = './' + config_file_name
    with open(config_file_path, 'r') as config_file:
      return json.load(config_file)
