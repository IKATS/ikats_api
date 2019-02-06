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

from unittest import TestCase

from ikats import IkatsAPI
from ikats.exceptions import IkatsNotFoundError
from ikats.objects import InOutParam


class TestOperator(TestCase):
    """
    Test Operator object
    """
    def test_nominal(self):
        """
        Get an Operator instance
        """
        api = IkatsAPI()

        op = api.op.get(name="slope_spark")
        self.assertIsNotNone(op.op_id)
        self.assertIsNotNone(op.desc)
        self.assertIsNotNone(op.label)
        self.assertEqual(1, len(op.inputs))
        self.assertEqual(InOutParam, type(op.inputs[0]))
        self.assertEqual(0, len(op.parameters))
        self.assertEqual(1, len(op.outputs))
        self.assertEqual(InOutParam, type(op.outputs[0]))

        with self.assertRaises(IkatsNotFoundError):
            api.op.results(pid=0)
