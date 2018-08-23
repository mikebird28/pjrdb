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
                v_name = contents[0].strip()
                v_typ = int(contents[1].strip())
                v_begin = int(contents[2].strip())
                v_end = int(contents[3].strip())
                v_default = contents[4].strip()
                if len(contents) > 5:
                    v_not_null = contents[5].strip() == "True"
                else:
                    v_not_null = False
                v_remarks = ",".join(contents[5:])
                value = Value(v_name,v_typ,v_begin,v_end,v_default,v_remarks,v_not_null)
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
    def __init__(self,name,typ,begin_at,end_at,default_value,remarks,not_null):
        self.name = name
        self.typ = typ
        self.begin_at = begin_at
        self.end_at = end_at
        #self.default_value = fit_type(self.typ,np.nan)
        self.default_value = np.nan
        self.remarks = remarks
        self.not_null = not_null

    def extract_value(self,line):
        v = line[self.begin_at:self.end_at]
        try:
            v = fit_type(self.typ,v)
        except ConvertException:
            if self.not_null:
                raise ConvertException
            else:
                v = self.default_value
        return v

def fit_type(typ,v):
    if typ == TYPE_INT:
        v = to_int(v)
    elif typ == TYPE_FLO:
        v = to_flo(v)
    elif typ == TYPE_CAT:
        v = to_cat(v)
    else:
        raise ConvertException("unknown type")
    return v

class ConvertException(Exception):
    pass

def to_int(v):
    try:
        iv = int(v)
        return iv
    except:
        raise ConvertException("failed to convert value to interger")

def to_flo(v):
    try:
        iv = float(v)
        return iv
    except:
        raise ConvertException("failed to convert value to float")

def to_cat(v):
    try:
        v = v.decode("cp932")
        iv = v.strip() + "s"
        return iv
    except:
        raise ConvertException("failed to convert value to categorical")

