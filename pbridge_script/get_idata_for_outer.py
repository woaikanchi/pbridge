#-*-coding:utf-8-*-
import sys
from tools import pto
import pickle
import os

extra_path=os.path.dirname(os.path.realpath(__file__))+'/..'

sql=sys.argv[1]
sql_name=sys.argv[2]
current=sys.argv[3]

data,columns=pto(sql.decode('utf-8'),raw=True)
content={'data':data,'columns':columns}

file_path=os.path.join(extra_path,'pbridge_tmp',sql_name+current+'tmp')

with open(file_path,'wb') as f:
    pickle.dump(content,f)
