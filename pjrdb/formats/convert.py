import sys

inp_path = sys.argv[1]

with open(inp_path,"r") as fp:
    for row in fp.readlines():
        row = row.strip().split('"')[1]
        print(",".join(row.split()))
    
