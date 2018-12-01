#-*- coding:utf-8 -*-
import re
import os
import sys
import time
import zipfile
import requests
import subprocess
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup

TYPE_EXPANDED_INFO = "expanded_info"
TYPE_TRAIN_INFO = "train_info"
TYPE_RACE_INFO = "race_info"
TYPE_LAST_INFO = "last_info"
TYPE_PAYOFF = "payoff"
TYPE_HORSE_INFO = "horse_info"
TYPE_HORSE_RESULT = "horse_result"
TYPE_HORSE_DETAIL = "horse_detail"
TYPE_COURSE_DETAIL = "course_detail"
TYPE_TRAIN_DETAIL = "train_detail"

TARGETS = [
    TYPE_EXPANDED_INFO,
    TYPE_TRAIN_INFO,
    TYPE_RACE_INFO,
    TYPE_LAST_INFO,
    TYPE_PAYOFF,
    TYPE_HORSE_INFO,
    TYPE_HORSE_RESULT,
    TYPE_HORSE_DETAIL,
    TYPE_COURSE_DETAIL,
    TYPE_TRAIN_DETAIL,
]

LATEST_URL = "http://www.jrdb.com/member/data/"

# Main function to get latest infrormation.
def fetch_latest_dataset(date,output_path,tmp_path,username,password):
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    if not os.path.exists(tmp_path):
        os.makedirs(tmp_path)

    # Datasets published previous day.
    year = str(date.year)[-2:]
    month = pad_num(date.month,2)
    day = pad_num(date.day,2)

    latest_path = "http://www.jrdb.com/member/data/Tyb/TYB{0}{1}{2}.lzh".format(year,month,day)
    jrdb_path = "http://www.jrdb.com/member/data/Jrdb/JRDB{0}{1}{2}.lzh".format(year,month,day)
    paci_path = "http://www.jrdb.com/member/data/Paci/PACI{0}{1}{2}.lzh".format(year,month,day)

    ls = [latest_path,jrdb_path,paci_path]
    for url in ls:
        status = fetch_zip(url,username,password,tmp_path)
        if status != 200:
            raise Exception("Cannot fetch {}. Status Code {}".format(url,status))

    downloaded_files = [fname for fname in os.listdir(tmp_path) if os.path.isfile(os.path.join(tmp_path,fname))]
    downloaded_files = [os.path.join(tmp_path,fname) for fname in downloaded_files]

    for file_path in downloaded_files:
        extract_file(file_path,output_path)

# Main function to get all past datasets
def fetch_all_datasets(root_path,username,password):
    f = Fetcher(username,password)
    for typ in TARGETS:
        dir_name = "compressed_" + typ
        f.fetch(typ,os.path.join(root_path,dir_name))

    #decompress all compressed datasets
    for typ in TARGETS:
        compressed_path = os.path.join(root_path,"compressed_" + typ)
        extract_path = os.path.join(root_path,typ)
        extract_files(compressed_path,extract_path)


# Fetch the list of datasets separted by date.
# This function should be used for "expanded_info","train_detail".
def fetch_list(url,username,password):
    url_list = []
    r = requests.get(url,auth = HTTPBasicAuth(username,password))
    status = r.status_code
    if status != 200:
        raise Exception("unable to fetch url list")
    html = r.text
    soup = BeautifulSoup(html,"html.parser")
    links = soup.select("li a")

    url_path = "/".join(url.split("/")[:-1])
    for link in links:
        file_name = link["href"]
        url_list.append("/".join([url_path,file_name]))
    return url_list

# Fetch the list of datasets separted by year
# This function is deprecated. You should use "fetch_dates_list" instead.
def fetch_years_list(url,username,password):
    url_list = []
    r = requests.get(url,auth = HTTPBasicAuth(username,password))
    status = r.status_code
    if status != 200:
        raise Exception("unable to fetch url list")
    html = r.text
    soup = BeautifulSoup(html,"html.parser")
    ul = soup.select("tr td ul")[0]
    links = ul.select("li a")

    url_path = "/".join(url.split("/")[:-1])
    for link in links:
        file_name = link["href"]
        url_list.append("/".join([url_path,file_name]))
    return url_list

# Fetch the list of datasets separted by date
def fetch_dates_list(url,username,password):
    url_list = []
    r = requests.get(url,auth = HTTPBasicAuth(username,password))
    html = r.text
    soup = BeautifulSoup(html,"html.parser")
    ul = soup.select("tr td ul")[1]
    links = ul.select("li a")
    url_path = "/".join(url.split("/")[:-1])

    for link in links:
        file_name = link["href"]
        url_list.append("/".join([url_path,file_name]))
    return url_list

class TypeInfo():
    """
    Function Holder to get list of target files
    """
    def __init__(self,root_url,ls_func):
        self.root_url = root_url
        self.ls_func = ls_func

class Fetcher():
    type_dict = {
        "horse_info"   : TypeInfo("http://www.jrdb.com/member/datazip/Kyi/index.html",fetch_dates_list),
        "horse_result" : TypeInfo("http://www.jrdb.com/member/datazip/Sed/index.html",fetch_dates_list),
        "expanded_info": TypeInfo("http://www.jrdb.com/member/data/Jrdb/index.html",fetch_list),
        "train_info"   : TypeInfo("http://www.jrdb.com/member/datazip/Cyb/index.html",fetch_dates_list),
        "race_info"    : TypeInfo("http://www.jrdb.com/member/datazip/Bac/index.html",fetch_dates_list),
        "last_info"    : TypeInfo("http://www.jrdb.com/member/datazip/Tyb/index.html",fetch_dates_list),
        "payoff"       : TypeInfo("http://www.jrdb.com/member/datazip/Hjc/index.html",fetch_dates_list),
        "horse_detail" : TypeInfo("http://www.jrdb.com/member/datazip/Ukc/index.html",fetch_dates_list),
        "course_detail": TypeInfo("http://www.jrdb.com/member/datazip/Kab/index.html",fetch_dates_list),
        "train_detail" : TypeInfo("http://www.jrdb.com/member/datazip/Cha/index.html",fetch_list),
    }

    def  __init__(self,username,password):
        self.username = username
        self.password = password

    def fetch(self,typ,dir_path,skip_exists = True):
        create_directory(dir_path)
        typc = self._type_context(typ)
        root_url = typc.root_url
        ls_func = typc.ls_func
        ls = ls_func(root_url,self.username,self.password)
        length = len(ls)
        print(typ)
        for count,url in enumerate(ls):
            file_exist = exist_file(dir_path,parse_filename(url))
            if file_exist and skip_exists:
                sys.stdout.write("({0}/{1}) {2} ...".format(count+1,length,url))
                sys.stdout.write("{0}\n".format("skipped"))
                sys.stdout.flush()
                continue
            sys.stdout.write("({0}/{1}) {2} ...".format(count+1,length,url))
            sys.stdout.flush()
            status = fetch_zip(url,self.username,self.password,dir_path)
            sys.stdout.write("{0}\n".format(status))
            sys.stdout.flush()
            time.sleep(1.0)

    def _type_context(self,typ):
        return self.type_dict[typ]

# Fetch zipped datasets from JRDB.
def fetch_zip(url,username,password,directory):
    r = requests.get(url,auth = HTTPBasicAuth(username,password))
    status = r.status_code
    if status != 200:
        return status
    content = r.content

    file_name = parse_filename(url)
    file_path = os.path.join(directory,file_name)
    with open(file_path,"wb") as fp:
        fp.write(content)
    return status

def parse_filename(url):
    filename = url.split("/")[-1]
    return filename

def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)

# Extract file
def extract_file(file_path,unzipped_path):
    create_directory(unzipped_path)
    if file_path.endswith("zip"):
        unzip(file_path,unzipped_path)
    elif file_path.endswith("lzh"):
        unlzh(file_path,unzipped_path)


# Extract all files whcih dir_path contains
def extract_files(dir_path,unzipped_path):
    ls = os.listdir(dir_path)
    path_ls = [os.path.join(dir_path,name) for name in ls]
    create_directory(unzipped_path)

    for path in path_ls:
        print(path)
        if path.endswith("zip"):
            unzip(path,unzipped_path)
        elif path.endswith("lzh"):
            unlzh(path,unzipped_path)
 
def unzip(file_path,unzipped_path):
    with zipfile.ZipFile(file_path,"r") as zf:
        zf.extractall(path = unzipped_path)

"""
def unlzh(file_path,unzipped_path):
    command = "lhasa xqw={0} {1}".format(unzipped_path,file_path)
    subprocess.call(command,shell=True)
"""

def remove_extension(s):
    s = re.sub(r"\..+","",s)
    print(s)
    return s

def unlzh(file_path,unzipped_path):
    command = "lha e -q -w {0} {1}".format(unzipped_path,file_path)
    subprocess.call(command,shell=True)


def exist_file(dir_path,fname):
    path = os.path.join(dir_path,fname)
    return os.path.exists(path)

def is_valid_account(username,password):
    url = "http://www.jrdb.com/member/n_index.html"
    r = requests.get(url,auth = HTTPBasicAuth(username,password))
    status = r.status_code
    if status == 200:
        return True
    else:
        return False

# Convert num to padded str.
# Ex. pad_num(7,2)=> 07,  pad_num(43,5) => 00043
def pad_num(num, length):
    num = str(num)[::-1]
    padded_num = ["0" for i in range(length)]

    for i in range(len(num)):
        padded_num[i] = num[i]
    padded_num = "".join(padded_num)[::-1]
    return padded_num
 
