
import argparse
import pandas as pd
from . import parse

DEFAULT_CSV_DIR = "./raw_csv"
DEFAULT_DB_PATH = "./race.db"
DEFAULT_OUTPUT_FILE = "./output.csv"

def main():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    parser = argparse.ArgumentParser(description = "Command-line tool to get formed jrdb csv.")
    subparsers = parser.add_subparsers()

    parser_fetch = subparsers.add_parser("fetch", help="Fetch all csv files from jrdb.")
    parser_fetch.add_argument("directory")
    parser.set_defaults(handler = command_fetch)

    parser_create = subparsers.add_parser("create", help="Create database from raw csv files.")
    parser_create.set_defaults(handler = command_create)

    parser_update = subparsers.add_parser("update", help="Fetch new csv files and append to database")
    parser_update.set_defaults(handler = command_update)

    parser_output = subparsers.add_parser("output", help="Generate formatted csv files.")
    parser_output.set_defaults(handler = command_output)

    #parser.add_argument("--debug", help = "run on debug mode", action = "store_true")
    #parser.add_argument("-o", "--output",help = "path to save output csv file", action = "store",type = str, default = "./output.csv")
    #parser.add_argument("-s", "--sql_path",help = "path to create cache db", action = "store",type = str, default = "./cache.db")
    args = parser.parse_args()
    if hasattr(args, 'handler'):
        args.handler(args)
    else:
        parser.print_help()

def command_fetch(args):
    pass

def command_create(args):
    print(args)
    parse.create_df()

def command_update(args):
    pass

def command_output(args):
    pass

if __name__ == "__main__":
    main()
