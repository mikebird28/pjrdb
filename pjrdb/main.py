
import argparse
import parse
import pandas as pd

def main():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", help = "run on debug mode", action = "store_true")
    parser.add_argument("-o", "--output",help = "path to save output csv file", action = "store",type = str, default = "./output.csv")
    parser.add_argument("-s", "--sql_path",help = "path to create cache db", action = "store",type = str, default = "./cache.db")
    args = parser.parse_args()
    parse.create_df(is_debug = args.debug,output_path = args.output, db_path = args.sql_path)

if __name__ == "__main__":
    main()
