#-*-coding:utf-8-*-
import sys
import pandas as pd
from pyhive import presto
import os

extra_path=os.path.dirname(os.path.realpath(__file__))+'/..'
sys.path.append(extra_path)

from pbridge_config.configs import PRESTO_CONFIG

def pto(sql,raw=False,config=PRESTO_CONFIG):
    try:
        conn=presto.connect(host=config['host'],port=config['port'])
        cur=conn.cursor()
        cur.execute(sql)
        data=cur.fetchall()
        columns=map(lambda x:x[0],cur.description)
    finally:
        conn.close()
    df=pd.DataFrame(data,columns=columns)
    print df.head()
    if raw:
        return data,columns
    else:
        return df
