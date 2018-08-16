
import time
import os
import pandas as pd
import numpy as np
import gc
import sqlite3
import tqdm
import csv_format

#remove duplicates before merege
drop_dups = [
    "hi_HorseID",
    "ri_RaceID",
    "hr_HorseID",
    "pre1_ResultID",
    "pre2_ResultID",
    "pre3_ResultID"
    "li_HorseID"
]

def create_df(is_debug = False):
    #load formats file
    formats = [
        csv_format.Format("horse_result","./formats/horse_result.csv"),
        csv_format.Format("horse_info","./formats/horse_info.csv"),
        csv_format.Format("race_info","./formats/race_info.csv"),
        csv_format.Format("latest_info","./formats/latest_info.csv"),
    ]

    df_dict = {}
    target_path = "./raw_text/"
    for f in formats:
        print("[*] parsing {}".format(f.name))
        parser = Parse(f,is_debug)
        path = os.path.join(target_path,f.name)
        
        df = parser.parse(path)
        df_dict[f.name] = df
    db_con = to_sql("./test.db",df_dict)
    del(df_dict);gc.coolect()

    df = merge_df(db_con)
    df.to_csv("./output.csv",index = False)
    df.iloc[0:1000,:].to_csv("./output_sample.csv")
    write_dtype_csv("./dtypes.csv",df)
    return df

def to_sql(db_path,df_dict):
    con = sqlite3.connect(db_path)
    for name,df in tqdm(df_dict.items()):
        df.to_sql(name,con,index = False, if_exists = "replace")
    return con

def merge_df(db_con):
    base_table = "horse_info"
    base_df = df_dict["horse_info"]
    add_prefix(base_df,"hi")
    del(df_dict["horse_info"])

    #define merge rules, pairs of (dataframe name, join rule)
    joints = [
        ("race_info",Joint("hi_RaceID","RaceID",None,"ri")),
        ("horse_result",Joint("hi_HorseID","HorseID",None,"hr")),
        ("latest_info",Joint("hi_HorseID","HorseID",None,"li")),
        ("horse_result",Joint("hi_Pre1ResultID","ResultID",None,"pre1")),
        ("horse_result",Joint("hi_Pre2ResultID","ResultID",None,"pre2")),
        ("horse_result",Joint("hi_Pre3ResultID","ResultID",None,"pre3")),
    ]
    df_count = {}
    for k,_ in joints:
        if k in df_count:
            df_count[k] = df_count[k] + 1
        else:
            df_count[k] = 1

    for l,j in joints:
        start_time = time.time()
        bef_len = len(base_df)
        base_df = j.merge(base_df,df_dict[l])
        aft_len = len(base_df)
        elapsed_time = time.time() - start_time
        print("length {} -> {}, elapsed time : {:.3f} sec".format(bef_len,aft_len,elapsed_time))

        left = df_count[l] - 1
        df_count[l] = left
        if left == 0:
            del(df_dict[l])
            gc.collect()
    return base_df

class Joint():
    def __init__(self,left_key,right_key,left_prefix = None,right_prefix = None):
        self.left_key = left_key
        self.right_key = right_key
        self.left_prefix = left_prefix
        self.right_prefix = right_prefix

    def merge(self,db_con,how = "left"):

        #optimze index to join
        if self.left_prefix is not None:
            add_prefix(left_df,self.left_prefix)
            left_key = self.left_prefix + "_" +self.left_key
        if self.right_prefix is not None:
            add_prefix(right_df,self.right_prefix)
            right_key = self.right_prefix + "_" +self.right_key

        #drop duplicate rows
        if left_key in drop_dups:
            left_df.dropna(subset = [left_key],inplace = True)
            left_df.drop_duplicates(left_key,inplace = True)
        if right_key in drop_dups:
            right_df.dropna(subset = [right_key],inplace = True)
            right_df.drop_duplicates(right_key,inplace = True)

        index_name = left_df.index.name
        is_none = False
        if index_name is None:
            left_df.index.name = "index"
            index_name = "index"
            is_none = True

        #change left_df index to target key
        left_df.reset_index(inplace = True, drop = False)
        left_df.set_index(left_key,drop = True, inplace = True)

        #change right_df index to target key
        right_df.set_index(right_key,inplace = True,drop = True)

        #join 
        left_df = left_df.join(right_df,how = "left")
        del(right_df)
        gc.collect()

        #reset left_df index to orignal value
        left_df.index.name = left_key
        left_df.reset_index(inplace = True, drop = False)
        left_df.set_index(index_name,inplace = True, drop = True)
        if is_none:
            left_df.index.name = None
        to_32bit(left_df)
        return left_df

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
            if self.is_debug and i > 10:
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

def add_prefix(db_con,table_name,prefix):
    ls = []
    for c in df.columns:
        if not c.startswith(prefix):
            c = prefix + "_" + c
        ls.append(c)
    df.columns = ls

def write_dtype_csv(path,df):
    with open(path,"w") as fp:
        for k,v in dict(df.dtypes).items():
            row = "{},{}\n".format(k,v)
            fp.write(row)
