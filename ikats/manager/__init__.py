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

from ikats.manager.generic_mgr_ import IkatsGenericApiEndPoint
from ikats.manager.operator_mgr_ import IkatsOperatorMgr
from ikats.manager.dataset_mgr import IkatsDatasetMgr
from ikats.manager.metadata_mgr_ import IkatsMetadataMgr
from ikats.manager.table_mgr_ import IkatsTableMgr
from ikats.manager.timeseries_mgr_ import IkatsTimeseriesMgr

__path__ = extend_path(__path__, __name__)
