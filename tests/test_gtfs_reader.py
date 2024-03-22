import unittest
from unittest.mock import patch, MagicMock
from data_reader.gtfs_reader import GTFSReader
import io

class TestGTFSReader(unittest.TestCase):
    def setUp(self):
        self.reader = GTFSReader()

    @patch('requests.get')
    @patch('zipfile.ZipFile')
    def test_download_and_extract_static_data(self, mock_zipfile, mock_requests_get):
        # Mock the response from requests.get
        mock_response = MagicMock()
        mock_response.content = b'fake_zip_content'
        mock_requests_get.return_value = mock_response

        # Mock the zipfile.ZipFile context manager
        mock_zip = MagicMock()
        mock_zip.extractall.return_value = None
        mock_zipfile.return_value.__enter__.return_value = mock_zip

        # Call the method under test
        self.reader.download_and_extract_static_data()

        # Assert that requests.get was called with the correct URL
        mock_requests_get.assert_called_once_with(self.reader.static_url)

        # Assert that zipfile.ZipFile was called with the correct content
        mock_zipfile.assert_called_once_with(io.BytesIO(b'fake_zip_content'))

        # Assert that extractall was called with the correct directory
        mock_zip.extractall.assert_called_once_with("temp_gtfs")

if __name__ == '__main__':
    unittest.main()