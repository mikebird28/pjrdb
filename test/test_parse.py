import unittest
import datetime
import os

from pjrdb import fetcher,parse

username = os.environ["jrdb_username"]
password = os.environ["jrdb_password"]

class TestFetcher(unittest.TestCase):
    def test_parse_latest_dataset(self):
        test_path = "./test_outputs/parse/"
        output_path = "./test_outputs/parse/extracted"
        tmp_path = "./test_outputs/parse/tmp_path"
        if not os.path.exists(test_path):
            os.makedirs(test_path)
        date = datetime.date(2018,12,1)
        fetcher.fetch_latest_dataset(date, output_path, tmp_path, username, password)
        parse.parse_latest_df(output_path)

