#import requests
import pandas as pd
from pandas import Series, DataFrame
import sqlite3
from pandas import ExcelWriter
import time
#import numpy as np
import math
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np

con = sqlite3.connect('C:/a/ext19yrs.db')


def DBlist(fileloc):
    con = sqlite3.connect(fileloc)
    cursor = con.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    Plist = cursor.fetchall()
    PL = []
    for i in range(0, len(Plist)): # DB안의 회사 이름을 PL에 저장/list format
        j = Plist[i][0]
        PL.append(j)
    return (PL)

yrs = DBlist('C:/a/C:/a/ext19yrs.db.db')


df = pd.read_sql("SELECT * FROM '%s'" % com_code, con)

y2014 = e.major_xs('2014')
y2015 = e.major_xs('2015')
y2016 = e.major_xs('2016')
y2017 = e.major_xs('2017')
y2018 = e.major_xs('2018')

conn = sqlite3.connect('C:/a/Ext2018_by_year.db')
y2014.to_sql('2014', conn, if_exists='replace', index_label='PAGECODE')
y2015.to_sql('2015', conn, if_exists='replace', index_label='PAGECODE')
y2016.to_sql('2016', conn, if_exists='replace', index_label='PAGECODE')
y2017.to_sql('2017', conn, if_exists='replace', index_label='PAGECODE')
y2018.to_sql('2018', conn, if_exists='replace', index_label='PAGECODE')

if __name__ == "__main__":
    col = ['B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U']
    col_small = ['C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U']

    con = sqlite3.connect('C:/a/ext19yrs.db')
    # nowtime = datetime.datetime.now().strftime('%Y%m%d%H%M')
    yrs = DBlist('C:/a/ext19yrs.db')  # 리스트 호출~

    fileloc = 'C:/a/ext19yrs.db'

    #e = dataload(fileloc)
    #company_screen = e.transpose(2, 0, 1)

    y2018 = company_screen.major_xs('2018')  # 한해 자료 다 뽑을때
    PPL = y2018.index
    #temp = e.major_xs(PPL[2])  # 한회사의 20년치 재무제표 다 뽑음.

    panel_book = {}

    for i, j in enumerate(PPL):
        raw = e.major_xs(j)
        df1 = reindex(raw)
        df = build_FS(df1)
        dup = dupont_analysis(df)
        wholeFS = pd.concat([df, dup], axis=0)
        CF = Build_CF(wholeFS)
        CF = CF.round(3)
        RA = Build_RA(wholeFS, CF)
        CR = Build_CR(wholeFS, CF)
        wholeFS = wholeFS.round(2)
        RA = RA.round(3)
        CR = CR.round(3)
        CF = CF.drop("U", axis=1)
        CF.columns = col_small
        Wholedata = pd.concat([wholeFS, CF, RA, CR], axis=0)
        Wholedata = Wholedata.drop(["B"], axis=1)
        Wholedata.columns = yrs[1:]
        temp_data = {j: Wholedata}
        panel_book.update(temp_data)
        print(j, temp_data)


