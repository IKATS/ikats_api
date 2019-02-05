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

import json

from ikats.lib import check_type
from ikats.objects.generic_ import IkatsObject


class InOutParam:
    """
    Details about the inputs/outputs/parameters of an operator
    """

    # TODO: Compléter docstring
    def __init__(self, api, json_data=None):
        """
        Constructor

        :param api:
        :param json_data:

        :type api:
        :type json_data:
        """

        self.__desc = None
        self.__domain = None
        self.__label = None
        self.__name = None
        self.__order_index = None
        self.__dtype = None
        self.__default_value = None
        self.__rid = None

        self.__api = None
        self.api = api

        if json_data is not None:
            check_type(value=json_data, allowed_types=dict, var_name="json_data", raise_exception=True)

            self.desc = json_data.get("description", None)
            self.domain = json_data.get("domain", None)
            self.label = json_data.get("label", None)
            self.name = json_data.get("name", None)
            self.order_index = json_data.get("order_index", None)
            self.dtype = json_data.get("type", None)
            self.default_value = json_data.get("default_values", None)

    @property
    def api(self):
        """
        IkatsAPI
        :rtype: IkatsAPI
        """
        return self.__api

    @api.setter
    def api(self, value):
        self.__api = value

    @property
    def rid(self):
        """
        Result ID
        :rtype: int or str
        """
        return self.__rid

    @rid.setter
    def rid(self, value):
        check_type(value=value, allowed_types=[int, str, None], var_name="rid", raise_exception=True)
        self.__rid = value

    @property
    def desc(self):
        """
        Input/Parameter/Output description
        :rtype: str
        """
        return self.__desc

    @desc.setter
    def desc(self, value):
        check_type(value=value, allowed_types=[str, None], var_name="desc", raise_exception=True)
        self.__desc = value

    @property
    def domain(self):
        """
        Domain of values (json list as str). Used for discrete list
        :rtype: str
        """
        return self.__domain

    @domain.setter
    def domain(self, value):
        if isinstance(value, str):
            value = json.loads(value)
        check_type(value=value, allowed_types=[list, None], var_name="domain", raise_exception=True)
        self.__domain = value

    @property
    def label(self):
        """
        Input/Parameter/Output displayed name
        :rtype: str
        """
        return self.__label

    @label.setter
    def label(self, value):
        check_type(value=value, allowed_types=[str, None], var_name="label", raise_exception=True)
        self.__label = value

    @property
    def name(self):
        """
        Input/Parameter/Output unique name
        :rtype: str
        """
        return self.__name

    @name.setter
    def name(self, value):
        check_type(value=value, allowed_types=[str, None], var_name="name", raise_exception=True)
        self.__name = value

    @property
    def order_index(self):
        """
        Input/Parameter/Output index in displayed order
        :rtype: int
        """
        return self.__order_index

    @order_index.setter
    def order_index(self, value):
        check_type(value=value, allowed_types=[int, None], var_name="order_index", raise_exception=True)
        self.__order_index = value

    @property
    def dtype(self):
        """
        Input/Parameter/Output data type
        :rtype: str
        """
        return self.__dtype

    @dtype.setter
    def dtype(self, value):
        check_type(value=value, allowed_types=[str, None], var_name="dtype", raise_exception=True)
        self.__dtype = value

    @property
    def default_value(self):
        """
        Input/Parameter/Output default value (or default selected value if domain is set)
        :rtype: str
        """
        return self.__default_value

    @default_value.setter
    def default_value(self, value):
        self.__default_value = value

    def get(self):
        """
        get the result in database and returns it (not kept locally, a call to backend is performed every time)
        Return type depends on result data type
        """
        if self.rid is None:
            raise ValueError("Provide a RID first")
        return self.api.op.result(rid=self.rid)


class Operator(IkatsObject):
    """
    Operator handles the static information of an IKATS operator
    """

    # TODO : Docstring
    def __init__(self, api, name=None):
        """
        See props for members description

        :param api: see IkatsObject
        :param name: name of the operator to construct

        :type api:
        :type name:
        """
        super().__init__(api)
        self.__name = None
        self.__label = None
        self.__desc = None
        self.__op_id = None
        self.__family = None

        self.__inputs = []
        self.__parameters = []
        self.__outputs = []

        self.name = name

    @property
    def family(self):
        """
        Family this operator belongs to
        :rtype: str
        """
        return self.__family

    @family.setter
    def family(self, value):
        check_type(value=value, allowed_types=[str, None], var_name="family", raise_exception=True)
        self.__family = value

    @property
    def inputs(self):
        """
        List of inputs used by this operator
        :rtype: list of InOutParam
        """
        return self.__inputs

    @inputs.setter
    def inputs(self, value):
        check_type(value=value, allowed_types=list, var_name="inputs", raise_exception=True)
        self.__inputs = value

    @property
    def parameters(self):
        """
        List of parameters used by this operator
        :rtype: list of InOutParam
        """
        return self.__parameters

    @parameters.setter
    def parameters(self, value):
        check_type(value=value, allowed_types=list, var_name="parameters", raise_exception=True)
        self.__parameters = value

    @property
    def outputs(self):
        """
        List of outputs used by this operator
        :rtype: list of InOutParam
        """
        return self.__outputs

    @outputs.setter
    def outputs(self, value):
        check_type(value=value, allowed_types=list, var_name="outputs", raise_exception=True)
        self.__outputs = value

    @property
    def label(self):
        """
        Short name of the operator used in GUI
        :rtype: str
        """
        return self.__label

    @label.setter
    def label(self, value):
        check_type(value=value, allowed_types=[str, None], var_name="label", raise_exception=True)
        self.__label = value

    @property
    def desc(self):
        """
        Short description of the operator
        :rtype: str
        """
        return self.__desc

    @desc.setter
    def desc(self, value):
        check_type(value=value, allowed_types=[str, None], var_name="desc", raise_exception=True)
        self.__desc = value

    @property
    def op_id(self):
        """
        Internal ID of the operator
        :rtype: int
        """
        return self.__op_id

    @op_id.setter
    def op_id(self, value):
        check_type(value=value, allowed_types=[str, int, None], var_name="op_id", raise_exception=True)
        if value is not None:
            value = int(value)
        self.__op_id = value

    @property
    def name(self):
        """
        Unique name of the operator
        :rtype: str
        """
        return self.__name

    @name.setter
    def name(self, value):
        check_type(value=value, allowed_types=[str, None], var_name="name", raise_exception=True)
        self.__name = value

    # TODO : doc
    def fetch(self):
        """
        If light content (no parameters/inputs/outputs specified), fetch the missing data
        and update the Operator object

        :rtype:
        """
        if self.name is None:
            raise ValueError("Provide an operator name to fetch")

        result = self.api.op.get(name=self.name)

        self.desc = result.desc
        self.label = result.label
        self.op_id = result.op_id
        self.inputs = result.inputs
        self.parameters = result.parameters
        self.outputs = result.outputs

    def __str__(self):
        return "Operator %s" % self.name

    def __repr__(self):
        return "Operator %s" % self.name


class RunOp(Operator):
    """
    Operator class with necessary elements to be runnable
    """
    # TODO: docstring
    def __init__(self, name):
        super().__init__(name)
        self.__pid = None
        self.__results = None

    @property
    def pid(self):
        """
        Unique identifier of the process ID
        :rtype: str or int
        """
        return self.__pid

    @pid.setter
    def pid(self, value):
        check_type(value=value, allowed_types=[int, None], var_name="pid", raise_exception=True)
        self.__pid = value

    # TODO: doc
    def results(self):
        """
        Reads the results pointers and assign them to thei respective outputs
        :rtype:
        """
        if self.pid is None:
            raise ValueError("Provide a PID first")
        results = self.api.op.results(pid=self.pid)
        for i, info in enumerate(results):
            self.outputs[i].rid = info["id"]

    def __str__(self):
        return "Running Operator %s" % self.name

    def __repr__(self):
        return "Running Operator %s" % self.name
