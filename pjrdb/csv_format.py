import numpy as np

TYPE_INT = 1
TYPE_FLO = 2
TYPE_CAT = 3

class Format():
    def __init__(self,name,path):
        self.name = name
        self.path = path
        self.values = self.__load()

    def __load(self):
        values = []
        with open(self.path) as fp:
            lines = fp.readlines()
            for l in lines:
                contents = l.split(",")
                v_name = contents[0]
                v_typ = int(contents[1])
                v_begin = int(contents[2])
                v_end = int(contents[3])
                v_default = contents[4]
                v_remarks = ",".join(contents[5:])
                value = Value(v_name,v_typ,v_begin,v_end,v_default,v_remarks)
                values.append(value)
        return values

    def parse_line(self,line):
        parsed_dic = {}
        for v in self.values:
            parsed_l = v.extract_value(line)
            parsed_dic[v.name] = parsed_l
        return parsed_dic

    def get_columns(self):
        return sorted([v.name for v in self.values])

class Value():
    def __init__(self,name,typ,begin_at,end_at,default_value,remarks):
        self.name = name
        self.typ = typ
        self.begin_at = begin_at
        self.end_at = end_at
        #self.default_value = fit_type(self.typ,np.nan)
        self.default_value = np.nan
        self.remarks = remarks

    def extract_value(self,line):
        v = line[self.begin_at:self.end_at]
        v = fit_type(self.typ,v,self.default_value)
        return v

def fit_type(typ,v,def_v = np.nan):
    if typ == TYPE_INT:
        v = to_int(v,def_v)
    elif typ == TYPE_FLO:
        v = to_flo(v,def_v)
    elif typ == TYPE_CAT:
        v = to_cat(v,def_v)
    else:
        raise Exception("unknown type")
    return v

def to_int(v,def_v):
    try:
        iv = int(v)
        return iv
    except:
        return def_v

def to_flo(v,def_v):
    try:
        iv = float(v)
        return iv
    except:
        return def_v

def to_cat(v,def_v):
    try:
        v = v.decode("cp932")
        iv = v.strip() + "s"
        if iv == "":
            return def_v
        else:
            return iv
    except Exception as e:
        return def_v

