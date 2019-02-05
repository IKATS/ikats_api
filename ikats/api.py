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
from ikats.manager import (IkatsDatasetMgr, IkatsMetadataMgr, IkatsOperatorMgr,
                           IkatsTableMgr, IkatsTimeseriesMgr)
from ikats.objects.session_ import IkatsSession


class IkatsAPI:
    """
    Ikats resources API

    Common library of endpoints used by algorithms developers & contributors to access the data handled by IKATS.
    """

    def __init__(self, host="http://localhost", port="80", sc=None, name="IKATS", session=None, emulate=False):
        """
        Constructor

        Let emulate to False since it is not fully implemented

        # TODO: Compléter avec la description et les types de chaque paramètre
        :param host:
        :param port:
        :param sc:
        :param name:
        :param session:
        :param emulate:
        
        :type host:
        :type port:
        :type sc:
        :type name:
        :type session:
        :type emulate:
        """
        self.__session = None
        if session is not None:
            self.session = session
        else:
            self.session = IkatsSession(host=host, port=port, sc=sc, name=name)

        # Emulate backend if set to True
        self.emulate = emulate

        self.ts = IkatsTimeseriesMgr(api=self)
        self.md = IkatsMetadataMgr(api=self)
        self.ds = IkatsDatasetMgr(api=self)
        self.op = IkatsOperatorMgr(api=self)
        self.table = IkatsTableMgr(api=self)

    @property
    def session(self):
        """
        Ikats Session information (connection to IKATS backend)
        :rtype: IkatsSession
        """
        return self.__session

    @session.setter
    def session(self, value):
        if isinstance(value, IkatsSession):
            self.__session = value
        else:
            raise TypeError("Type of session shall be IkatsSession, not %s" % (type(value)))

    def __repr__(self):
        return "IKATS API"
