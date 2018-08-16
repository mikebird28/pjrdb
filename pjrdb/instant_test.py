import parse
import csv_format
import unittest


class FormatTest(unittest.TestCase):
    def test_latest_info(self):
        f = csv_format.Format("latest_info","./formats/latest_info.csv")
        target_file = "./test_files/latest_info/"
        show_parse_result(f,target_file)

    def test_horse_info(self):
        f = csv_format.Format("latest_info","./formats/latest_info.csv")
        target_file = "./test_files/latest_info/"
        show_parse_result(f,target_file)

def show_parse_result(f, path):
    is_debug = True
    parser = parse.Parse(f,is_debug)

    df = parser.parse(path)
    for c in df.columns:
        name = c
        value = df.loc[0,c]
        print("{} : {}".format(name,value))


if __name__ == "__main__":
    unittest.main()
