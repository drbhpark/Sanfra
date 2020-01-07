#kisvalue 다운데이터를 한데 모으는 프로그램.12.16확인
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

#df = pd.read_excel('C:/a/kiss.xlsx', sheet_name = '2016', index_col = 'KIS')
df18 = pd.read_excel('C:/a/ext/2018s.xlsx')
df17 = pd.read_excel('C:/a/ext/2017s.xlsx')
df16 = pd.read_excel('C:/a/ext/2016s.xlsx')
df15 = pd.read_excel('C:/a/ext/2015s.xlsx')
df14 = pd.read_excel('C:/a/ext/2014s.xlsx')
df13 = pd.read_excel('C:/a/ext/2013s.xlsx')
df12 = pd.read_excel('C:/a/ext/2012s.xlsx')
df11 = pd.read_excel('C:/a/ext/2011s.xlsx')
df10 = pd.read_excel('C:/a/ext/2010s.xlsx')
df09 = pd.read_excel('C:/a/ext/2009s.xlsx')
df08 = pd.read_excel('C:/a/ext/2008s.xlsx')
df07 = pd.read_excel('C:/a/ext/2007s.xlsx')
df06 = pd.read_excel('C:/a/ext/2006s.xlsx')
df05 = pd.read_excel('C:/a/ext/2005s.xlsx')
df04 = pd.read_excel('C:/a/ext/2004s.xlsx')
df03 = pd.read_excel('C:/a/ext/2003s.xlsx')
df02 = pd.read_excel('C:/a/ext/2002s.xlsx')
df01 = pd.read_excel('C:/a/ext/2001s.xlsx')
df00 = pd.read_excel('C:/a/ext/2000s.xlsx')

length = len(df18)
col = list(df18.columns)
con = sqlite3.connect('C:/a/ext1905byKISticker18.db')

kiscode = list(df18['KIS'])
df17.index = df17['KIS']
df16.index = df16['KIS']
df15.index = df15['KIS']
df14.index = df14['KIS']
df18.index = df18['KIS']
df13.index = df13['KIS']
df12.index = df12['KIS']
df11.index = df11['KIS']
df10.index = df10['KIS']
df09.index = df09['KIS']
df08.index = df08['KIS']
df07.index = df07['KIS']
df06.index = df06['KIS']
df05.index = df05['KIS']
df04.index = df04['KIS']
df03.index = df03['KIS']
df02.index = df02['KIS']
df01.index = df01['KIS']
df00.index = df00['KIS']



for i in kiscode : #range(1,(length-1)):
     dfa = pd.DataFrame(columns=['2013','2014', '2015', '2016', '2017','2018'], index=col)
     try:
          dfa['2017'] = df17.ix[i]
          dfa['2016'] = df16.ix[i]
          dfa['2015'] = df15.ix[i]
          dfa['2014'] = df14.ix[i]
          dfa['2013'] = df13.ix[i]
          dfa['2018'] = df18.ix[i]
             #title = str(int(dfa.ix['Stock']['2016'])).zfill(6) #6자리로 맞춤.

          #title = dfa.ix['Stock']['2016'] #이건 상장사만 따로 할때, 티커명으로

          #if np.isnan(title) == False: # 상장사 티커가 없는건 제외하는 루틴
          #     title = str(int(title)).zfill(6)  # 6자리로 맞춤

          title = dfa.ix['KIS']['2018']  # 회사kis code로; 전체 다 할때 사용 - 중복된회사가 많다

          #if dfa.ix['Stock']['2016'] == 'nan' :
          #     pass
          #else :
            #   title = dfa.ix['Stock']['2016']  # 회사이름으로
          print(title)
               # dfa[3:].to_sql(title, con, if_exists='replace')
          dfa.to_sql(title, con, if_exists='replace')
     except: pass







