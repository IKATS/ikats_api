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

from ikats.lib import (check_is_fid_valid, check_is_valid_ds_name,
                       check_is_valid_epoch, check_type)


class TestUtils(TestCase):
    """
    Common library tests
    """
    def test_check_type(self):
        """
        Test check_type function
        """

        # Good types
        self.assertTrue(check_type(value=None, allowed_types=[None], var_name="my_None"))
        self.assertTrue(check_type(value="text", allowed_types=[str, None], var_name="my_str"))
        self.assertTrue(check_type(value=3, allowed_types=[int, None], var_name="my_int"))
        self.assertTrue(check_type(value=[], allowed_types=[list, None], var_name="my_list"))
        self.assertTrue(check_type(value={}, allowed_types=[dict, None], var_name="my_dict"))
        self.assertTrue(check_type(value="text", allowed_types=[str], var_name="my_str"))

        # Bad types
        self.assertFalse(check_type(value=42, allowed_types=[str, None], var_name="my_str", raise_exception=False))

        with self.assertRaises(TypeError):
            check_type(value=42, allowed_types=[str, None], var_name="my_int")

        with self.assertRaises(TypeError):
            check_type(value="text", allowed_types=[int, list], var_name="my_str")

    def test_check_is_valid_epoch(self):
        """
        Test check_is_valid_epoch function
        """

        # Value is not an int
        value = "abc"
        with self.assertRaises(TypeError):
            check_is_valid_epoch(value=value, raise_exception=True)
        self.assertFalse(check_is_valid_epoch(value=value, raise_exception=False))

        # Negative time
        value = -123
        with self.assertRaises(ValueError):
            check_is_valid_epoch(value=value, raise_exception=True)
        self.assertFalse(check_is_valid_epoch(value=value, raise_exception=False))

        # Valid time
        value = 123
        self.assertTrue(check_is_valid_epoch(value=value, raise_exception=False))

    # noinspection PyTypeChecker
    def test_check_is_valid_ds_name(self):
        """
        Test check_is_valid_ds_name function
        """

        # DS name not a str
        ds_name = 123
        with self.assertRaises(TypeError):
            check_is_valid_ds_name(value=ds_name, raise_exception=True)
        self.assertFalse(check_is_valid_ds_name(value=ds_name, raise_exception=False))

        # DS name too short
        ds_name = "a"
        with self.assertRaises(ValueError):
            check_is_valid_ds_name(value=ds_name, raise_exception=True)
        self.assertFalse(check_is_valid_ds_name(value=ds_name, raise_exception=False))

        # DS name too short
        ds_name = ""
        with self.assertRaises(ValueError):
            check_is_valid_ds_name(value=ds_name, raise_exception=True)
        self.assertFalse(check_is_valid_ds_name(value=ds_name, raise_exception=False))

        # DS name contains spaces
        ds_name = "DS name contains spaces"
        with self.assertRaises(ValueError):
            check_is_valid_ds_name(value=ds_name, raise_exception=True)
        self.assertFalse(check_is_valid_ds_name(value=ds_name, raise_exception=False))

        # Valid DS Name
        ds_name = "azerty"
        self.assertTrue(check_is_valid_ds_name(value=ds_name, raise_exception=False))

    # noinspection PyTypeChecker
    def test_check_is_fid_valid(self):
        """
        Test check_is_fid_valid function
        """

        # FID name not a str
        fid_value = 123
        with self.assertRaises(TypeError):
            check_is_fid_valid(fid=fid_value, raise_exception=True)
        self.assertFalse(check_is_fid_valid(fid=fid_value, raise_exception=False))

        # FID too short
        fid_value = "a"
        with self.assertRaises(ValueError):
            check_is_fid_valid(fid=fid_value, raise_exception=True)
        self.assertFalse(check_is_fid_valid(fid=fid_value, raise_exception=False))

        # FID too short
        fid_value = ""
        with self.assertRaises(ValueError):
            check_is_fid_valid(fid=fid_value, raise_exception=True)
        self.assertFalse(check_is_fid_valid(fid=fid_value, raise_exception=False))

        # FID contains spaces
        fid_value = "FID contains spaces"
        with self.assertRaises(ValueError):
            check_is_fid_valid(fid=fid_value, raise_exception=True)
        self.assertFalse(check_is_fid_valid(fid=fid_value, raise_exception=False))

        # Valid FID
        fid_value = "azerty"
        self.assertTrue(check_is_fid_valid(fid=fid_value, raise_exception=False))
