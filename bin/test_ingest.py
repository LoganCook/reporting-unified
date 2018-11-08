import unittest
from unittest.mock import Mock, patch
import requests

from ingest import read_conf, Ingester


class IngestTestCase(unittest.TestCase):
    @patch('ingest.AWS', return_value=None)
    def test_list_ingested(self, mock_aws):
        conf = read_conf('example-config.json')
        ingester = Ingester(conf)
        self.assertIsNotNone(ingester)

        # 200 with empty [] should be OK
        with patch('requests.get', return_value=Mock(status_code=200)) as mock_get:
            mock_get.return_value.json.return_value = []
            empty = ingester.list_ingested()
            self.assertIsInstance(empty, list)
            self.assertEqual(len(empty), 0)

        # 404 with empty body should be OK
        with patch('requests.get', return_value=Mock(status_code=404)) as mock_get:
            mock_get.return_value.json.return_value = ''
            empty = ingester.list_ingested()
            self.assertIsInstance(empty, list)
            self.assertEqual(len(empty), 0)

        with patch('requests.get', return_value=Mock(status_code=500)) as mock_get:
            mock_get.return_value.json.return_value = ''
            with self.assertRaises(OSError) as cm:
                empty = ingester.list_ingested()
            self.assertEqual(str(cm.exception), 'HTTP 500')

        with patch('requests.get', return_value=Mock(status_code=200)) as mock_get:
            mock_get.return_value.json.return_value = [{'name': 'one'}, {'name': 'two'}]
            result = ingester.list_ingested()
            self.assertIsInstance(result, list)
            self.assertEqual(len(result), 2)
