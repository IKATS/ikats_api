"""
Copyright 2018 CS Systèmes d'Information

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""
# Simplify the import patterns for user
# Instead of importing :
#   from ikats.session.TemporalDataMgr import TemporalDataMgr
# User will do:
#   from ikats.session import TemporalDataMgr


from pkgutil import extend_path

from ikats.client.utils import build_json_files, TEMPLATES, close_files
from ikats.client.generic_client import GenericClient
from ikats.client.ntdm_client import NTDMClient
from ikats.client.tdm_client import TDMClient

__path__ = extend_path(__path__, __name__)
