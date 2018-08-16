
import time
import os
import pandas as pd
import numpy as np
import gc
import sqlite3
from tqdm import tqdm
import csv_format

PARSE_FILE_NUM_ON_DEBUG = 10

formats = [
    csv_format.Format("horse_result","./formats/horse_result.csv"),
    csv_format.Format("horse_info","./formats/horse_info.csv"),
    csv_format.Format("race_info","./formats/race_info.csv"),
    csv_format.Format("latest_info","./formats/latest_info.csv"),
    csv_format.Format("extra_info","./formats/extra_info.csv"),
]

#remove duplicates before merege
drop_dict = {
    "horse_result" : ["HorseID","ResultID"],
    "horse_info" : ["HorseID"],
    "race_info" : ["RaceID"],
    "latest_info" : ["HorseID"],
    "extra_info" : ["HorseID"],
}

def create_df(is_debug = False, output_path = "./output.csv", db_path = "./cache.db"):
    #load formats file
    df_dict = {}
    target_path = "./raw_text/"
    for f in formats:
        print("[*] parsing {}".format(f.name))
        parser = Parse(f,is_debug)
        path = os.path.join(target_path,f.name)
        
        df = parser.parse(path)
        df_dict[f.name] = df.copy()
        del(df);gc.collect()

    print("[*] drop duplicated rows")
    start_time = time.time()
    drop_duplicated(df_dict,drop_dict)
    elapsed_time = time.time() - start_time
    print(elapsed_time)

    print("[*] creating sql database")
    start_time = time.time()
    db_con = to_sql("./test.db",df_dict)
    elapsed_time = time.time() - start_time
    print(elapsed_time)
    del(df_dict);gc.collect()
    #print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

    print("[*] generating output dataset")
    start_time = time.time()
    count = 0

    with open(output_path,"a") as f:
        #pbar = tqdm(total=100)
        is_first_write = True
        for df in merge_df(db_con,chunk_size = 10000):
        #    pbar.update(1)
            count += 1
            print(count * 10000)
            if is_first_write:
                df.to_csv(f,index = False,header = True)
                is_first_write = False
            else:
                df.to_csv(f,index = False,header = False)
        #pbar.close()
    write_dtype_csv("./dtypes.csv",df)
    elapsed_time = time.time() - start_time
    print(elapsed_time)
    return df

def to_sql(db_path,df_dict):
    con = sqlite3.connect(db_path)
    for name,df in tqdm(df_dict.items()):
        df.to_sql(name,con,index = False, if_exists = "replace")
    return con

def merge_df(db_con,chunk_size):
    orm = ORM(db_con, "output")
    joints = [
        Joint("horse_info","horse_result","HorseID","HorseID","hi","hr",how = "inner"),
        Joint("horse_info","race_info","RaceID","RaceID","hi","ri"),
        Joint("horse_info","latest_info","HorseID","HorseID","hi","li"),
        Joint("horse_info","extra_info","HorseID","HorseID","hi","ei"),
        Joint("horse_info","horse_result","Pre1ResultID","ResultID","hi","pre1"),
        Joint("horse_info","horse_result","Pre2ResultID","ResultID","hi","pre2"),
        Joint("horse_info","horse_result","Pre3ResultID","ResultID","hi","pre3"),
    ]
    for j in joints:
        orm.join(j)
    df = orm.execute(chunk_size)
    return df

def drop_duplicated(df_dict,drop_dict):
    keys = df_dict.keys()
    for name in keys:
        drop_targets = drop_dict[name]
        for drop_target in drop_targets:
            df = df_dict[name]
            bef_len = len(df)
            df = df[~df.duplicated(subset = drop_target)]
            aft_len = len(df)
            df_dict[name] = df.copy()
            del(df);gc.collect()
            print("drop dups of {}, {} => {}".format(name,bef_len,aft_len))
    return df_dict

class Joint():
    def __init__(self,left_table,right_table,left_key,right_key,left_prefix = None, right_prefix = None, how = "left"):
        self.left_table = left_table
        self.right_table = right_table
        self.left_key = left_key
        self.right_key = right_key
        self.left_prefix = left_prefix
        self.right_prefix = right_prefix

        join_dict = {
            "left" : "LEFT JOIN",
            "inner" : "INNER JOIN",
        }
        self.join_type = join_dict[how]

class ORM():
    def __init__(self,db_con,table_name):
        self.con = db_con
        self.table_name = table_name
        self.join_rules = []

    def join(self,joint):
        self.join_rules.append(joint)

    def _rename_columns(self,df,columns):
        new_columns = []
        for c in columns:
            splited = c.split(".")
            prefix = splited[0]
            column_name = ",".join(splited[1:])
            new_column_name = "{}_{}".format(prefix,column_name)
            new_columns.append(new_column_name)
        df.columns = new_columns

    def _get_column_list(self,table_name):
        columns = []
        query = "SELECT * FROM {0}".format(table_name)
        cur = self.con.execute(query)
        for column in cur.description:
            name = column[0]
            columns.append(name)
        cur.close()
        return columns

    def _create_index(self,join_rules):
        #create indexes for left_table
        table_name = None
        target_keys = []
        for rule in join_rules:
            if table_name is  None:
                table_name = rule.left_table
            if table_name != rule.left_table:
                raise Exception("cannot specify multiple left tabel")
            key = rule.left_key
            target_keys.append(key)
        target_keys = sorted(list(set(target_keys)))
        key_names = ",".join(target_keys)
        index_name = "{}_main_index".format(table_name)
        query = "CREATE INDEX '{}' ON '{}' ({});".format(index_name,table_name,key_names)
        self.con.execute(query)

        #create indexes for right table
        indexes = [] #ls of (table_name,key_name) tuples
        for rule in join_rules:
            table_name = rule.right_table
            key = rule.right_key
            indexes.append((table_name,key))

        indexes = sorted(list(set(indexes)))

        for table_name,key_name in indexes:
            index_name = "{}_{}_index".format(table_name,key_name)
            query = "CREATE INDEX '{}' ON '{}' ('{}');".format(index_name,table_name,key_name)
            self.con.execute(query)

    def execute(self,chunk_size):
        join_queries = []
        target_columns = []

        self._create_index(self.join_rules)

        for rules in self.join_rules:
            table_name   = rules.right_table
            left_prefix  = rules.left_prefix
            right_prefix = rules.right_prefix
            join_type = rules.join_type
            left_columns = self._get_column_list(rules.left_table)
            right_columns = self._get_column_list(rules.right_table)

            #add prefix to join keys
            if left_prefix is None:
                left_key = rules.left_key
            else:
                left_key = "{}.{}".format(left_prefix,rules.left_key)
                left_columns = ["{}.{}".format(left_prefix,c) for c in left_columns]

            if right_prefix is None:
                right_key = rules.right_key
            else:
                right_key = "{}.{}".format(right_prefix,rules.right_key)
                right_columns = ["{}.{}".format(right_prefix,c) for c in right_columns]

            target_columns.extend(right_columns)
            target_columns.extend(left_columns)
            join_query = "{} {} as {} ON {} = {}".format(join_type,table_name,right_prefix,left_key,right_key)
            join_queries.append(join_query)

        #drop duplicates and sorte target columns
        target_columns = sorted(list(set(target_columns)))
        columns_query = ",".join(target_columns)
        join_query = " ".join(join_queries)
        query = "SELECT {} FROM horse_info as hi {};".format(columns_query,join_query)

        for df in pd.read_sql_query(query,self.con,chunksize = chunk_size):
            self._rename_columns(df,target_columns)
            yield df

class Parse():
    def __init__(self,formats,is_debug = False):
        self.formats = formats
        self.is_debug = is_debug

    def parse(self,target_dir):
        start = time.time()
        df = create_empty_df(self.formats)
        records = []
        files = os.listdir(target_dir)
        files = list(filter(lambda x:x.endswith(".txt"),files))

        for i, path in enumerate(files):
            if self.is_debug and i > PARSE_FILE_NUM_ON_DEBUG:
                break
            path = os.path.join(target_dir,path)
            records.extend(parse_file(self.formats,path))

        df = df.append(records)
        print(time.time() - start)
        df = optimize_dtypes(df,self.formats)
        return df

def parse_file(formats,path):
    ls = []
    with open(path,"rb") as fp:
        for line in fp.readlines():
            ls.append(formats.parse_line(line))
    return ls

def optimize_dtypes(df,formats):
    func_dict = {
        csv_format.TYPE_INT : (lambda df : df.astype(np.float32)),
        csv_format.TYPE_FLO : (lambda df : df.astype(np.float32)),
        csv_format.TYPE_CAT : (lambda df : df.astype(str)),
    }
    for v in formats.values:
        df.loc[:,v.name] = func_dict[v.typ](df[v.name])
    return df

def to_32bit(df):
    for c in df.columns:
        dtype = df.dtypes[c]
        if dtype == float:
            df.loc[:,c] = df.loc[:,c].astype(np.float32)
        elif dtype == int:
            df.loc[:,c] = df.loc[:,c].astype(np.int32)
        else:
            #Do Nothing
            pass

def create_empty_df(formats):
    columns = formats.get_columns()
    df = pd.DataFrame(columns = columns)
    return df

def write_dtype_csv(path,df):
    with open(path,"w") as fp:
        for k,v in dict(df.dtypes).items():
            row = "{},{}\n".format(k,v)
            fp.write(row)
