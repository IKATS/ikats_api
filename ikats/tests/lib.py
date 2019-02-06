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

from ikats import IkatsAPI
from ikats.exceptions import IkatsNotFoundError

def delete_ts_if_exists(fid):
    """
    Delete a TS if it exists
    Nothing is return
    Useful to prepare environments

    :param fid: FID of the TS to delete
    :type fid: str
    """
    api = IkatsAPI()

    try:
        ts = api.ts.get(fid=fid)
        return api.ts.delete(ts=ts, raise_exception=False)
    except IkatsNotFoundError:
        return True
