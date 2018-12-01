import parse
import csv_format
import unittest
import os
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


class FormatTest(unittest.TestCase):
    def test_latest_info(self):
        f = csv_format.Format("latest_info","./formats/latest_info.csv")
        df = get_df(f,"TYB")
        #drop dups
        print(df.describe())
        #print(df["EquipmentChange"])
        df = df.loc[~df.duplicated(),:]
        dups = df.loc[df.duplicated(subset = "HorseID",keep = False),:]
        dups_num = len(dups)
        print(len(dups))
        if  dups_num != 0:
            print("output duplicated rows to 'dups.csv")
            dups = dups.sort_values(by = "HorseID")
            dups.to_csv("dups.csv")

    def test_extra_info(self):
        f = csv_format.Format("extra_info","./formats/extra_info.csv")
        df = get_df(f,"kka")
        print(df.head())
        print(df.describe())

        dups = df.loc[df.duplicated(subset = "HorseID",keep = False),:]
        dups_num = len(dups)
        print(len(dups))
        if  dups_num != 0:
            print("output duplicated rows to 'dups.csv")
            dups = dups.sort_values(by = "HorseID")
            dups.to_csv("dups.csv")

def get_df(f,prefix):
    is_debug = True
    target_path = "./raw_text/"
    parser = parse.Parse(f,is_debug,prefix)
    path = os.path.join(target_path,f.name)
    df = parser.parse(path)
    return df

if __name__ == "__main__":
    unittest.main()
