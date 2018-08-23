import pandas as pd
import parse
import csv_format
import unittest


class Checker(unittest.TestCase):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.df = pd.read_csv("output.csv")

    def test_irregal_rows(self):
        cdf = self.df.loc[self.df.loc[:,"hr_HorseName"] != self.df.loc[:,"hi_HorseName"],:]
        targets = ["hr_HorseName","hi_HorseName","pre1_HorseName"]
        print(cdf.loc[:,targets])
        print(len(self.df))
        print(len(cdf))

    def test_irregal_race(self):
        pass

if __name__ == "__main__":
    unittest.main()
