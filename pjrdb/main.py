
import argparse
import pandas as pd
from . import parse,fetcher

DEFAULT_CSV_DIR = "./raw_csv"
DEFAULT_DB_PATH = "./race.db"
DEFAULT_OUTPUT_FILE = "./output.csv"

def main():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    parser = argparse.ArgumentParser(description = "Command-line tool to get formed jrdb csv.")
    subparsers = parser.add_subparsers()

    parser_fetch = subparsers.add_parser("fetch", help="Fetch all csv files from jrdb.")
    parser_fetch.add_argument("directory",type=str)
    parser_fetch.add_argument("-u","--username",type=str, required=True)
    parser_fetch.add_argument("-p","--password",type=str, required=True)
    parser.set_defaults(handler = command_fetch)

    parser_create = subparsers.add_parser("create", help="Create database from raw csv files.")
    parser_fetch.add_argument("-o","--output",type=str,help="output path of created database")
    parser_fetch.add_argument("-d","--csv-directory",type=str,help="directory which csv files are saved")
    parser_create.set_defaults(handler = command_create)

    parser_update = subparsers.add_parser("update", help="Fetch new csv files and append to database")
    parser_update.set_defaults(handler = command_update)

    parser_output = subparsers.add_parser("output", help="Generate formatted csv files.")
    parser_output.set_defaults(handler = command_output)

    args = parser.parse_args()
    if hasattr(args, 'handler'):
        args.handler(args)
    else:
        parser.print_help()

def command_fetch(args):
    root_dir = args.directory
    username = args.username
    password = args.password
    fetcher.fetch_all_datasets(root_dir,username,password)

def command_create(args):
    db_path = args.db_path
    csv_dir = args.csv_dir
    parse.create_database(db_path,csv_dir)

def command_update(args):
    pass

def command_output(args):
    pass

if __name__ == "__main__":
    main()
