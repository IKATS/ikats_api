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
from ikats.session_ import IkatsSession
from ikats.manager.operator_mgr_ import IkatsOperatorMgr
from ikats.manager.dataset_mgr import IkatsDatasetMgr
from ikats.manager.metadata_mgr_ import IkatsMetadataMgr
from ikats.manager.process_data_mgr_ import IkatsProcessDataMgr
from ikats.manager.table_mgr_ import IkatsTableMgr
from ikats.manager.timeseries_mgr_ import IkatsTimeseriesMgr


class IkatsAPI:
    """
    Ikats resources API

    Common library of endpoints used by algorithms developers & contributors to access the data handled by IKATS.
    """

    def __init__(self, host="http://localhost", port="80", sc=None, name="IKATS", session=None):
        self.__session = None
        if session is not None:
            self.session = session
        else:
            self.session = IkatsSession(host=host, port=port, sc=sc, name=name)

        self.ts = IkatsTimeseriesMgr(api=self)
        self.md = IkatsMetadataMgr(api=self)
        self.ds = IkatsDatasetMgr(api=self)
        self.op = IkatsOperatorMgr(api=self)
        self.pd = IkatsProcessDataMgr(api=self)
        self.table = IkatsTableMgr(api=self)

    @property
    def session(self):
        return self.__session

    @session.setter
    def session(self, value):
        if type(value) == IkatsSession:
            self.__session = value
        else:
            raise TypeError("Type of session shall be IkatsSession, not %s" % (type(value)))

    def __repr__(self):
        return "IKATS API"
