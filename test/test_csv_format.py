import unittest

from pjrdb import csv_format


class TestCsvFormat(unittest.TestCase):
    def test_format(self):
        pairs = [
            ("horse_result","formats/horse_result.csv"),
            ("basic_info","formats/basic_info.csv"),
            ("horse_info","formats/horse_info.csv"),
            ("race_info","formats/race_info.csv"),
            ("latest_info","formats/latest_info.csv"),
            ("extra_info","formats/extra_info.csv"),
            ("train_info","formats/train_info.csv"),
        ]
        for name,path in pairs:
            csv_format.Format(name,path)

if __name__ == "__main__":
    unittest.main()

