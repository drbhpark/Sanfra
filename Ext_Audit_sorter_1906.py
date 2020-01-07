#외감기관 자료 분석 프로그램

import sys
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5 import uic

import pandas as pd
from pandas import Series, DataFrame
import sqlite3
from pandas import ExcelWriter
import time
import numpy as np
import math
import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import random
import scipy.stats as stats


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

def evalchart(yr, fileloce):
    con = sqlite3.connect(fileloce)
    df = pd.read_sql("SELECT * FROM '%s'" % yr, con, index_col= 'index')
    #df.fillna(value=0, inplace=True)
    #df = df.T
    return df

def dataload(fileloc): #초기 데이터 로딩부터 시작
    eva = {}             # 빠른 연습을 위해 모집단 줄임
    for k, l in enumerate(yrs): #여기까지 히스토리 전 데이터, F&M score관련 팩터들, eval을 메모리에 올림.
        #print(k,len(PPL))
        eva.update({l: evalchart(l,fileloc)})

    e = pd.Panel(eva)
    return e


#Terminal = 0.03  #Terminal sales growth - 산업에 따라 다르고 업체에따라 다르나 7%는 넘기지 말도록 /가장 중요한 Assumption
col = ['B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q','R','S','T','U']

collen = len(col)



if __name__ == "__main__":
    col = ['B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U']
    coll = ['B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U']

    col_small = ['C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U']

    con = sqlite3.connect('C:/a/ext19yrs_evalfull.db')
    yrs = DBlist('C:/a/ext19yrs_evalfull.db')  # 리스트 호출~
    fileloc = 'C:/a/ext19yrs_evalfull.db'

    e = dataload(fileloc)
    company_screen = e.transpose(2,0,1)

    y2018 = company_screen.major_xs('2018')    #한해 자료 다 뽑을때
    PPL = y2018.index
    temp = e.major_xs(PPL[2]) #한회사의 20년치 재무제표 다 뽑음.











