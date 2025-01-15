import unittest
from unittest.mock import MagicMock, patch

from hyrun.result import Result, ResultManager, get_result


class TestResult(unittest.TestCase):
    """Test Result class."""

    def setUp(self):
        """Set up."""
        self.result = Result(a=1, b=2, c=3)

    def test_initialization(self):
        """Test that the Result is initialized correctly."""
        self.assertEqual(self.result.a, 1)
        self.assertEqual(self.result.b, 2)
        self.assertEqual(self.result.c, 3)

    def test_getitem(self):
        """Test getting an item."""
        self.assertEqual(self.result['a'], 1)
        self.assertEqual(self.result['b'], 2)
        self.assertEqual(self.result['c'], 3)

    def test_setitem(self):
        """Test setting an item."""
        self.result['d'] = 4
        self.assertEqual(self.result.d, 4)
        self.assertEqual(self.result['d'], 4)

    def test_delitem(self):
        """Test deleting an item."""
        del self.result['a']
        with self.assertRaises(AttributeError):
            _ = self.result.a
        with self.assertRaises(KeyError):
            _ = self.result['a']

    def test_len(self):
        """Test the length of the Result."""
        self.assertEqual(len(self.result), 3)
        self.result['d'] = 4
        self.assertEqual(len(self.result), 4)

    def test_contains(self):
        """Test if an item is in the Result."""
        self.assertIn('a', self.result)
        self.assertNotIn('d', self.result)

    def test_get(self):
        """Test getting an item with a default value."""
        self.assertEqual(self.result.get('a'), 1)
        self.assertEqual(self.result.get('d', 4), 4)

    def test_items(self):
        """Test getting items."""
        self.assertEqual(list(self.result.items()),
                         [('a', 1), ('b', 2), ('c', 3)])

    def test_keys(self):
        """Test getting keys."""
        self.assertEqual(list(self.result.keys()), ['a', 'b', 'c'])

    def test_values(self):
        """Test getting values."""
        self.assertEqual(list(self.result.values()), [1, 2, 3])


class TestGetResult(unittest.TestCase):
    """Test get_result function."""

    def setUp(self):
        """Set up."""
        self.result = {'key': 'value'}
        self.result_list = [{'key': 'value1'}, {'key': 'value2'}]
        self.logger = MagicMock()

    def test_get_result_dict(self):
        """Test get_result with dict output."""
        result = get_result(self.result, output_type='dict',
                            logger=self.logger)
        self.assertEqual(result, self.result)

    def test_get_result_result(self):
        """Test get_result with Result output."""
        result = get_result(self.result, output_type='Result',
                            logger=self.logger)
        self.assertIsInstance(result, Result)
        self.assertEqual(result.get('key'), self.result['key'])

    def test_get_result_yaml(self):
        """Test get_result with yaml output."""
        result = get_result(self.result, output_type='yaml',
                            logger=self.logger)
        self.assertIsInstance(result, str)
        self.assertIn('key: value', result)

    def test_get_result_json(self):
        """Test get_result with json output."""
        result = get_result(self.result, output_type='json',
                            logger=self.logger)
        self.assertIsInstance(result, str)
        self.assertIn('"key": "value"', result)

    def test_get_result_list(self):
        """Test get_result with a list of results."""
        result = get_result(self.result_list, output_type='dict',
                            logger=self.logger)
        self.assertIsInstance(result, list)
        self.assertEqual(result, self.result_list)

    @patch('hytools.units.convert_units',
           return_value={'key': 'converted_value'})
    def test_get_result_with_unit_conversion(self, mock_convert_units):
        """Test get_result with unit conversion."""
        result = get_result(self.result,
                            output_type='dict', logger=self.logger,
                            units_to='new_unit', units_from='old_unit')
        self.assertEqual(result, {'key': 'converted_value'})
        mock_convert_units.assert_called_once()


class TestResultManager(unittest.TestCase):
    """Test ResultManager class."""

    def setUp(self):
        """Set up."""
        self.result = {'key': 'value'}
        self.logger = MagicMock()
        self.rm = ResultManager(logger=self.logger, output_type='dict')

    def test_convert_units(self):
        """Test convert_units method."""
        with patch('hytools.units.convert_units',
                   return_value={'key': 'converted_value'}
                   ) as mock_convert_units:
            result = self.rm.convert_units(self.result)
            self.assertEqual(result, {'key': 'converted_value'})
            mock_convert_units.assert_called_once()

    def test_convert_to_dict(self):
        """Test convert_to_dict method."""
        result = self.rm.convert_to_dict(self.result)
        self.assertEqual(result, self.result)

        class TestObject:
            """Test object with __dict__ attribute."""

            def __init__(self):
                """Initialize."""
                self.key = 'value'

        obj = TestObject()
        result = self.rm.convert_to_dict(obj)
        self.assertEqual(result, {'key': 'value'})

        with self.assertRaises(TypeError):
            self.rm.convert_to_dict('invalid')

    def test_dump(self):
        """Test dump method."""
        result = self.rm.dump(self.result)
        self.assertEqual(result, self.result)

        self.rm.output_type = 'Result'
        result = self.rm.dump(self.result)
        self.assertIsInstance(result, Result)

        self.rm.output_type = 'yaml'
        result = self.rm.dump(self.result)
        self.assertIsInstance(result, str)
        self.assertIn('key: value', result)

        self.rm.output_type = 'json'
        result = self.rm.dump(self.result)
        self.assertIsInstance(result, str)
        self.assertIn('"key": "value"', result)


if __name__ == '__main__':
    unittest.main()
