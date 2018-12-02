import unittest
import datetime
import os

# Test targets
from pjrdb import fetcher

username = os.environ["jrdb_username"]
password = os.environ["jrdb_password"]

class TestFetcher(unittest.TestCase):
    def test_pad_num(self):
        self.assertEqual(fetcher.pad_num(12,2), "12")
        self.assertEqual(fetcher.pad_num(1,2), "01")
        self.assertEqual(fetcher.pad_num(123,5), "00123")

    def test_fetch_latest_dataset(self):
        tmp_path = "./test_outputs/zipped"
        output_path = "./test_outputs/extracted"
        date = datetime.date(2018,12,1)
        fetcher.fetch_latest_dataset(date,output_path,tmp_path,username,password)

    def test_fetch_all_dataset(self):
        download_path = "./test_outputs/fetch"
        fetcher.fetch_all_datasets(download_path,username,password,3)

if __name__ == "__main__":
    unittest.main()
