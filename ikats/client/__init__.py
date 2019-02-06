# -*- coding: utf-8 -*-
"""
Copyright 2019 CS Systèmes d'Information

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
from pkgutil import extend_path

from ikats.client.datamodel_client import DatamodelClient
from ikats.lib import MDType
from ikats.client.generic_client import GenericClient
from ikats.client.generic_client import is_404, is_4xx, is_5xx, check_type, check_http_code
from ikats.client.opentsdb_client import OpenTSDBClient
from ikats.client.catalog_client import CatalogClient

__path__ = extend_path(__path__, __name__)
