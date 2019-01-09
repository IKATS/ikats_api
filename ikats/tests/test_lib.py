from unittest import TestCase
from ikats.lib import check_type


class TestUtils(TestCase):
    def test_check_type_good(self):
        self.assertTrue(check_type(value="text", allowed_types=[str, None], var_name="my_str"))
        self.assertTrue(check_type(value=3, allowed_types=[int, None], var_name="my_int"))
        self.assertTrue(check_type(value=[], allowed_types=[list, None], var_name="my_list"))
        self.assertTrue(check_type(value={}, allowed_types=[dict, None], var_name="my_dict"))
        self.assertTrue(check_type(value=None, allowed_types=[None], var_name="my_None"))
        self.assertTrue(check_type(value="text", allowed_types=[str], var_name="my_str"))

    def test_check_type_wrong(self):
        self.assertFalse(check_type(value=42, allowed_types=[str, None], var_name="my_str", raise_exception=False))

    def test_check_type_wrong_with_raise(self):
        with self.assertRaises(TypeError):
            check_type(value=42, allowed_types=[str, None], var_name="my_int")

        with self.assertRaises(TypeError):
            check_type(value="text", allowed_types=[int, list], var_name="my_str")
