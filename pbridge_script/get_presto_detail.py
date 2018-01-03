#-*-coding:utf-8-*-
from tools import pto
import pickle

presto_detail={}

databases=pto("""show schemas""")['Schema']
for db in databases:
    presto_detail[db]={}
    tbs=pto("""show tables from {0}""".format(db))['Table']
    for tb in tbs:
        presto_detail[db][tb]=None
        try:
            des=map(lambda x:list(x),pto("""describe {0}""".format(db+'.'+tb)).values)       
            presto_detail[db][tb]=des
        except:
            continue

with open('../pbridge_files/presto_detail.pickle','wb') as f:
    pickle.dump(presto_detail,f)    
