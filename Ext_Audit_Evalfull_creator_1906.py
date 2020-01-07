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
    df = pd.read_sql("SELECT * FROM '%s'" % yr, con, index_col= 'KIS')
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



def reindex(data):
# 항목매칭작업
    temp = {
        '0 Stockcode' : 'ticker',
        '00 Company Name' : 'company_name',
        '01 Sales': 'sales',
        '02 COGS': 'COGS',
        '03 RDExpense': 'RDExpense',
        '04 SGA': 'SGA',
        '05 Depreciation and Amortization': 'DeAm',
        '06 Interest_Expenses': 'Interest_Exp',
        '07 Non operational income': 'Nonop',  # 영업외수익
        '08 Income Tax': 'incomeTax',  # 법인세비용
        '09 Minority Interest in Earning' :'Min_Int_Earning',            # 지분법 이익
        '10 Other Income(Loss)' :  'Otherincome',                             # 기타수익
        '11 Extra Items or discontinued business income': 'ExtItems',  # 특별기타항목-통상적이지 않은 이익이나 매각 등 - 중단사업이익
        '12 Preferred Dividend': 'PreferredDiv', #자본잉여금 감소의 기타부분
        '13 Operating Cash and Marketable Securiteis': 'Opc_Msec',  # 현금및현금성자산
        '14 Receivables': 'Receivables',  # 매출채권
        '15 Inventories': 'Inventories',  # 재고자산
        '16 Other Current Assets': 'OCA',  # 기타유동자산
        '17 PP&E': 'PPE',  # 설비자산
        '18 Investments': 'Investment',  # 투자자산
        '19 Intangibles': 'Intangible',  # 무형자산
        '20 Other Assets': 'Otherassets',  # 기타비유동자산
        '21 Current Debt': 'CurrentDebt',  # 유동부채
        '22 Account Payable': 'Acc_payable',  # 매입채무
        '23 Income Tax Payable': 'IncomeTax_payable',  # 이연법인세부채
        '24 Other Current Liabilities': 'OthCurrLiab',  # 기타유동부채
        '25 Long term debt': 'longtermdebt',
        '26 Other Liabilities': 'OthLiab',
        '27 Deferred Tax': 'DefTax',  # 이연법인세부채
        '28 Minority Interests': 'Minority_interest',  # 외부주주지분
        '29 Preferred Stock': 'Pref_stock',  # 우선주자본금 eval p188
        '30 Paid in Common Capital': 'Paidincommcap',  # 보통주자본금
        '31 Retained Earnings': 'Retained_earnings',  # 이익잉여금
        '32 Common Dividends': 'Common_div',#,  # 배당금의지급
        '33 Number of Common Stocks' : 'N_CommStock'
          }

    data.rename(index = temp, inplace = True)
    return data



# 받아올 데이터 포맷 정리 루틴
def build_FS(data):
    #data2 = data1.drop('2017/12(E)', axis=1)
    #data = data1.rename(columns={'2014': 'B', '2015': 'C', '2016': 'D', '2017':'E', '2018': 'F'})  # 인덱스 변경
    data.columns = col
    sales = data.ix['sales']
    COGS = data.ix['COGS']
    RDExpense = data.ix['RDExpense']
    SGA = data.ix['SGA']
    DeAm = data.ix['DeAm']
    Interest_Exp = data.ix['Interest_Exp']
    Nonop = data.ix['Nonop']
    Min_Int_Earning = data.ix['Min_Int_Earning']
    incomeTax = data.ix['incomeTax']
    Otherincome = data.ix['Otherincome']
    ExtItems= data.ix['ExtItems']
    PreferredDiv = data.ix['PreferredDiv']
    Opc_Msec = data.ix['Opc_Msec']
    Receivables =  data.ix['Receivables']
    Inventories =  data.ix['Inventories']
    OCA = data.ix['OCA']
    PPE = data.ix['PPE']
    Investment = data.ix['Investment']
    Intangible = data.ix['Intangible']
    Otherassets = data.ix['Otherassets']
    CurrentDebt = data.ix['CurrentDebt']
    Acc_payable = data.ix['Acc_payable']
    IncomeTax_payable = data.ix['IncomeTax_payable']
    OthCurrLiab =  data.ix['OthCurrLiab']
    longtermdebt = data.ix['longtermdebt']
    OthLiab = data.ix['OthLiab']
    DefTax =  data.ix['DefTax']
    Paidincommcap = data.ix['Paidincommcap']
    Retained_earnings= data.ix['Retained_earnings']
    Minority_interest = data.ix['Minority_interest']
    Pref_stock =  data.ix['Pref_stock']
    Common_div = data.ix['Common_div']
    N_CommStock = data.ix['N_CommStock']
    Nega_com_div = - Common_div
    #coll = ['B','C','D','E','F']

    # finalize Income Statement
    GrossProfit = sales + COGS
    EBITDA = sales + COGS + RDExpense + SGA
    EBIT = sales + COGS + RDExpense + SGA + DeAm
    EBT = EBIT + Interest_Exp + Nonop
    NetIncomeBEI = EBT + incomeTax + Otherincome
    NetIncome = NetIncomeBEI + ExtItems + PreferredDiv + Min_Int_Earning
    # finalize Balance sheet
    TCurAss = Opc_Msec + Receivables + Inventories + OCA
    TAsset = TCurAss + PPE + Investment + Intangible + Otherassets
    TCurLiab = CurrentDebt + Acc_payable + IncomeTax_payable + OthCurrLiab
    TLiab = TCurLiab + longtermdebt + OthLiab + DefTax
    TotCommEquity = Paidincommcap + Retained_earnings
    TotLiabEquity = TLiab + TotCommEquity + Minority_interest + Pref_stock

    F62 = Series(index=col)
    F63 = Series(index=col)
    F64 = Series(index=col)
    F65 = Series(index=col)
    F66 = Series(index=col)

    for i in range (1,20):
        F62[coll[i]] = Retained_earnings[coll[i-1]]
        F63[coll[i]] = NetIncome[coll[i]]
        F64[coll[i]] = - Common_div[coll[i]]
        F66[coll[i]] = Retained_earnings[coll[i]]
        F65[coll[i]] = F66[coll[i]] - F62[coll[i]] - F63[coll[i]] - F64[coll[i]]



    temp = {'F12': sales,
        'F13': COGS,
        'F14': GrossProfit,
        'F15': RDExpense,
        'F16': SGA,
        'F17' : EBITDA,
        'F18': DeAm,
        'F19' : EBIT,
        'F20': Interest_Exp,
        'F21': Nonop,  # 영업외수익
        'F22' : EBT,
        'F23': incomeTax,  # 법인세비용
        'F24' :  Otherincome,                             # 기타수익
        'F25' : NetIncomeBEI,
        'F26': ExtItems,  # 특별기타항목-통상적이지 않은 이익이나 매각 등 - 중단사업이익
        'F27' : Min_Int_Earning, # 수정함
        'F28': PreferredDiv, #자본잉여금 감소의 기타부분
        'F29' : NetIncome,
        'F33': Opc_Msec,  # 현금및현금성자산
        'F34': Receivables,  # 매출채권
        'F35': Inventories,  # 재고자산
        'F36': OCA,  # 기타유동자산
        'F37' : TCurAss, # 총 유동자산
        'F38': PPE,  # 설비자산
        'F39': Investment,  # 투자자산
        'F40': Intangible,  # 무형자산
        'F41': Otherassets,  # 기타비유동자산
        'F42' : TAsset, # 총자산
        'F44': CurrentDebt,  # 유동부채
        'F45': Acc_payable,  # 매입채무
        'F46': IncomeTax_payable,  # 이연법인세부채
        'F47': OthCurrLiab,  # 기타유동부채
        'F48' : TCurLiab, # 총유동부채
        'F49': longtermdebt,
        'F50': OthLiab,
        'F51': DefTax,  # 이연법인세부채
        'F52': TLiab, # 총부채
        'F53': Minority_interest,  # 외부주주지분
        'F54': Pref_stock,  # 우선주자본금 eval p188
        'F55': Paidincommcap,  # 보통주자본금
        'F56': Retained_earnings,  # 이익잉여금
        'F57' : TotCommEquity,
        'F58' : TotLiabEquity,
        'F64': Nega_com_div,  # 배당금의지급
        'F62' : F62,
        'F63' : F63,
        'F65' : F65,
        'F66' : F66,
        'F05' : N_CommStock
            }

    df1 = pd.DataFrame(temp)      #dataframe으로 모으고
    df2 = df1.transpose()        #축 변경

    return (df2)


def dupont_analysis(data):

        F71 = Series(index=col)
        F72 = Series(index=col)
        F73 = Series(index=col)
        F74 = Series(index=col)
        F75 = Series(index=col)
        F76 = Series(index=col)
        F77 = Series(index=col)

        F77 = data.ix['F57']
        F76 = data.ix['F44'] + data.ix['F49'] + data.ix['F54'] + data.ix['F53']
        F75 = F76 + F77
        try:
            F74 = - data.ix['F23'] / data.ix['F22']
        except:
            F74 = 0
        F73 = data.ix['F29']
        F72 = data.ix['F20'] * (1 - F74) + data.ix['F28'] + data.ix['F27']
        F71 = F73 - F72

        df = pd.DataFrame({'F71':F71, 'F72': F72, 'F73': F73, 'F74': F74, 'F75': F75, 'F76': F76, 'F77': F77})
        df1 = df.transpose()
        return df1


def Build_CF(f):

    CF11 = Series(index=col)
    CF12 = Series(index=col)
    CF13 = Series(index=col)
    CF14 = Series(index=col)
    CF15 = Series(index=col)
    CF16 = Series(index=col)
    CF17 = Series(index=col)
    CF18 = Series(index=col)
    CF19 = Series(index=col)
    CF20 = Series(index=col)
    CF21 = Series(index=col)
    CF22 = Series(index=col)
    CF23 = Series(index=col)
    CF24 = Series(index=col)
    CF27 = Series(index=col)
    CF28 = Series(index=col)
    CF29 = Series(index=col)
    CF30 = Series(index=col)
    CF31 = Series(index=col)
    CF34 = Series(index=col)
    CF35 = Series(index=col)
    CF36 = Series(index=col)
    CF37 = Series(index=col)
    CF38 = Series(index=col)
    CF39 = Series(index=col)
    CF40 = Series(index=col)
    CF41 = Series(index=col)
    CF43 = Series(index=col)
    CF44 = Series(index=col)
    CF45 = Series(index=col)
    CF50 = Series(index=col)
    CF51 = Series(index=col)
    CF52 = Series(index=col)
    CF53 = Series(index=col)
    CF56 = Series(index=col)
    CF57 = Series(index=col)
    CF58 = Series(index=col)
    CF59 = Series(index=col)
    CF60 = Series(index=col)
    CF61 = Series(index=col)
    CF62 = Series(index=col)
    CF63 = Series(index=col)
    CF64 = Series(index=col)
    CF67 = Series(index=col)
    CF68 = Series(index=col)
    CF69 = Series(index=col)
    CF74 = Series(index=col)
    CF75 = Series(index=col)
    CF76 = Series(index=col)
    CF77 = Series(index=col)
    CF80 = Series(index=col)
    CF81 = Series(index=col)
    CF82 = Series(index=col)
    CF83 = Series(index=col)
    CF84 = Series(index=col)
    CF85 = Series(index=col)
    CF86 = Series(index=col)
    CF89 = Series(index=col)
    CF90 = Series(index=col)
    CF91 = Series(index=col)
    CF92 = Series(index=col)
    CF93 = Series(index=col)
    CF94 = Series(index=col)
    CF95 = Series(index=col)
    CF96 = Series(index=col)
    CF97 = Series(index=col)
    CF100 = Series(index=col)
    CF101 = Series(index=col)
    CF102 = Series(index=col)
    CF103 = Series(index=col)
    CF104 = Series(index=col)
    CF105 = Series(index=col)
    CF106 = Series(index=col)
    CF107 = Series(index=col)
    CF108 = Series(index=col)
    CF109 = Series(index=col)
    CF110 = Series(index=col)
    CF111 = Series(index=col)
    CF112 = Series(index=col)
    CF113 = Series(index=col)
    CF114 = Series(index=col)
    CF115 = Series(index=col)
    CF116 = Series(index=col)
    CF120 = Series(index=col)
    CF121 = Series(index=col)
    CF122 = Series(index=col)
    CF124 = Series(index=col)
    CF125 = Series(index=col)
    CF126 = Series(index=col)
    CF127 = Series(index=col)


#annual growth rates
    for i in range(0,19):
    #Operating
        try:
            CF11[col[i]] = f.at['F29', col[i+1]]
            CF12[col[i]] = -f.at['F18', col[i + 1]]
            CF13[col[i]] = f.at['F51', col[i + 1]] - f.at['F51', col[i]]
            CF14[col[i]] = f.at['F50', col[i + 1]] - f.at['F50', col[i]]
            CF15[col[i]] = -f.at['F27', col[i + 1]]
            CF16[col[i]] = -f.at['F28', col[i + 1]]
            CF17[col[i]] = CF11[col[i]] + CF12[col[i]] + CF13[col[i]] + CF14[col[i]] + CF15[col[i]] + CF16[col[i]]
            CF18[col[i]] = -(f.at['F34', col[i + 1]] - f.at['F34', col[i]])
            CF19[col[i]] = -(f.at['F35', col[i + 1]] - f.at['F35', col[i]])
            CF20[col[i]] = -(f.at['F36', col[i + 1]] - f.at['F36', col[i]])
            CF21[col[i]] = f.at['F45', col[i + 1]] - f.at['F45', col[i]]
            CF22[col[i]] = f.at['F46', col[i + 1]] - f.at['F46', col[i]]
            CF23[col[i]] = f.at['F47', col[i + 1]] - f.at['F47', col[i]]
            CF24[col[i]] = CF17[col[i]] + CF18[col[i]] + CF19[col[i]] + CF20[col[i]] + CF21[col[i]] + CF22[col[i]] + CF23[col[i]]
        #Investing
            CF27[col[i]] = - (f.at['F38', col[i + 1]] - f.at['F38', col[i]] - f.at['F18', col[i+1]])
            CF28[col[i]] = - (f.at['F39', col[i + 1]] - f.at['F39', col[i]])
            CF29[col[i]] = - (f.at['F40', col[i + 1]] - f.at['F40', col[i]])
            CF30[col[i]] = - (f.at['F41', col[i + 1]] - f.at['F41', col[i]])
            CF31[col[i]] = CF27[col[i]] + CF28[col[i]] + CF29[col[i]] + CF30[col[i]]

        #Financing
            CF34[col[i]] = f.at['F44', col[i + 1]] + f.at['F49', col[i+1]] -  f.at['F44', col[i]] - f.at['F49', col[i]]
            CF35[col[i]] = f.at['F27', col[i + 1]] + f.at['F53', col[i + 1]] - f.at['F53', col[i]]
            CF36[col[i]] = f.at['F28', col[i + 1]]
            CF37[col[i]] = f.at['F54', col[i + 1]] - f.at['F54', col[i]]
            CF38[col[i]] = f.at['F64', col[i + 1]]
            CF39[col[i]] = f.at['F55', col[i + 1]] - f.at['F55', col[i]]
            CF40[col[i]] = f.at['F65', col[i + 1]]
            CF41[col[i]] = CF34[col[i]] + CF35[col[i]] + CF36[col[i]] + CF37[col[i]] + CF38[col[i]] + CF39[col[i]] + CF40[col[i]]
            CF43[col[i]] = CF24[col[i]] + CF31[col[i]] + CF41[col[i]]
            CF44[col[i]] = f.at['F33', col[i]]
            CF45[col[i]] = CF43[col[i]] + CF44[col[i]]

        #Free Cash Flow to Common Equity
            CF50[col[i]] = f.at['F29', col[i + 1]]
            CF51[col[i]] = - (f.at['F57', col[i + 1]] - f.at['F57', col[i]])
            CF52[col[i]] = f.at['F65', col[i + 1]]
            CF53[col[i]] = CF50[col[i]] + CF51[col[i]] + CF52[col[i]]

            CF56[col[i]] = CF24[col[i]]
            CF57[col[i]] = -CF43[col[i]]
            CF58[col[i]] = CF31[col[i]]
            CF59[col[i]] = CF34[col[i]]
            CF60[col[i]] = CF35[col[i]]
            CF61[col[i]] = f.at['F28', col[i + 1]]
            CF62[col[i]] = CF37[col[i]]
            CF63[col[i]] = f.at['F65', col[i + 1]]
            CF64[col[i]] = CF56[col[i]] + CF57[col[i]] + CF58[col[i]] + CF59[col[i]] + CF60[col[i]] + CF61[col[i]] + CF62[col[i]] + CF63[col[i]]

            CF67[col[i]] = - f.at['F64', col[i + 1]]
            CF68[col[i]] = - (f.at['F55', col[i + 1]] - f.at['F55', col[i]])
            CF69[col[i]] = CF67[col[i]] + CF68[col[i]]

            CF74[col[i]] =  f.at['F71', col[i + 1]]
            CF75[col[i]] = - (f.at['F75', col[i + 1]] - f.at['F75', col[i]])
            CF76[col[i]] = f.at['F65', col[i + 1]]
            CF77[col[i]] = CF74[col[i]] + CF75[col[i]] + CF76[col[i]]

            CF80[col[i]] = CF24[col[i]]
            CF81[col[i]] = - (f.at['F33', col[i + 1]] - f.at['F33', col[i]])
            CF82[col[i]] = CF31[col[i]]
            CF83[col[i]] = - (f.at['F20', col[i + 1]])

            if  (f.at['F22', col[i + 1]])==0 :
                CF84[col[i]] = 0
            else:
                CF84[col[i]] = -(-(f.at['F23', col[i + 1]]/f.at['F22', col[i + 1]]) * CF83[col[i]])
            CF85[col[i]] = f.at['F65', col[i + 1]]
            CF86[col[i]] = CF80[col[i]] + CF81[col[i]] + CF82[col[i]] + CF83[col[i]] + CF84[col[i]] + CF85[col[i]]
        #financing flows
            CF89[col[i]] = - f.at['F64', col[i + 1]]
            CF90[col[i]] = - f.at['F20', col[i + 1]]

            if (f.at['F22', col[i + 1]]) == 0:
                CF91[col[i]] = 0
            else:
                CF91[col[i]] = -(-(f.at['F23', col[i + 1]] / f.at['F22', col[i + 1]]) * CF90[col[i]])

            CF92[col[i]] = - f.at['F28', col[i + 1]]
            CF93[col[i]] = - f.at['F27', col[i + 1]] - (f.at['F53', col[i+1]] - f.at['F53', col[i]] )
            CF94[col[i]] = - (f.at['F55', col[i + 1]] - f.at['F55', col[i]])
            CF95[col[i]] = -(f.at['F44', col[i + 1]] + f.at['F49', col[i + 1]] - f.at['F44', col[i]] - f.at['F49', col[i]])
            CF96[col[i]] = -(f.at['F54', col[i + 1]] - f.at['F54', col[i]])
            CF97[col[i]] = CF89[col[i]] + CF90[col[i]] + CF91[col[i]] + CF92[col[i]] + CF93[col[i]] + CF94[col[i]] + CF95[col[i]] + CF96[col[i]]

        #Tranditional computation of FCF
            CF100[col[i]] = f.at['F19', col[i + 1]]

            if (f.at['F22', col[i + 1]]) == 0:
                CF101[col[i]] = f.at['F23', col[i + 1]]
            else:
                CF101[col[i]] = f.at['F23', col[i + 1]] -(f.at['F23', col[i + 1]] / f.at['F22', col[i + 1]]) * f.at['F20', col[i + 1]]
            CF102[col[i]] = f.at['F51', col[i + 1]] - f.at['F51', col[i]]
            CF103[col[i]] = CF100[col[i]] + CF101[col[i]] + CF102[col[i]]
            CF104[col[i]] = - f.at['F18', col[i + 1]]
            CF105[col[i]] = f.at['F21', col[i + 1]]
            CF106[col[i]] = f.at['F24', col[i + 1]]
            CF107[col[i]] = f.at['F26', col[i + 1]]
            CF108[col[i]] = CF103[col[i]] + CF104[col[i]] + CF105[col[i]] + CF106[col[i]] + CF107[col[i]]
            CF109[col[i]] = -(f.at['F37', col[i + 1]] - f.at['F37', col[i]]) +(f.at['F48', col[i+1]] - f.at['F44', col[i+1]] - f.at['F48', col[i]] + f.at['F44', col[i]] )
            CF110[col[i]] = -(f.at['F38', col[i + 1]] - f.at['F38', col[i]] - f.at['F18', col[i + 1]])
            CF111[col[i]] = -(f.at['F39', col[i + 1]] - f.at['F39', col[i]])
            CF112[col[i]] = -(f.at['F40', col[i + 1]] - f.at['F40', col[i]])
            CF113[col[i]] = -(f.at['F41', col[i + 1]] - f.at['F41', col[i]])
            CF114[col[i]] = f.at['F50', col[i + 1]] - f.at['F50', col[i]]
            CF115[col[i]] = f.at['F65', col[i + 1]]
            CF116[col[i]] = CF108[col[i]] + CF109[col[i]] + CF110[col[i]] + CF111[col[i]] + CF112[col[i]] + CF113[col[i]] + CF114[col[i]]+ CF115[col[i]]

            CF120[col[i]] =  ((f.at['F37',col[i+1]] - f.at['F33',col[i+1]] - f.at['F48',col[i+1]] + f.at['F44',col[i+1]]) -
                           (f.at['F37',col[i]] - f.at['F33',col[i]] - f.at['F48',col[i]] + f.at['F44',col[i]])) / (f.at['F75',col[i]] - f.at['F33',col[i]])
            CF121[col[i]] = ((f.at['F42', col[i + 1]] - f.at['F37', col[i + 1]] - f.at['F52', col[i + 1]] + f.at['F49', col[i + 1]]+ f.at['F48', col[i + 1]] + f.at['F53', col[i + 1]]  ) -
                         (f.at['F42', col[i]] - f.at['F37', col[i]] - f.at['F52', col[i]] + f.at['F49', col[i]]+f.at['F48', col[i]] + f.at['F53', col[i]])) / (
                        f.at['F75', col[i]] - f.at['F33', col[i]])
            CF122[col[i]] = (f.at['F75', col[i + 1]] - f.at['F33', col[i + 1]] - f.at['F75', col[i]] + f.at['F33', col[i]])/( f.at['F75', col[i]] - f.at['F33', col[i]])
            CF124[col[i]] = f.at['F12', col[i + 1]] / f.at['F12', col[i]] - 1

            CF125[col[i]] = -((f.at['F12', col[i + 1]]/(f.at['F57', col[i + 1]] + f.at['F54', col[i + 1]] + f.at['F49', col[i + 1]]
                            + f.at['F44', col[i + 1]] + f.at['F53', col[i + 1]] - f.at['F33', col[i + 1]])) -
                              (f.at['F12', col[i]] / (f.at['F57', col[i]] + f.at['F54', col[i]] + f.at['F49', col[i]]
                              + f.at['F44', col[i]] + f.at['F53', col[i]] - f.at['F33', col[i]]))) / (f.at['F12', col[i + 1]]/(f.at['F57', col[i + 1]] + f.at['F54', col[i + 1]] + f.at['F49', col[i + 1]]
                            + f.at['F44', col[i + 1]] + f.at['F53', col[i + 1]] - f.at['F33', col[i + 1]]))
            CF126[col[i]] = CF124[col[i]] * CF125[col[i]]
            CF127[col[i]] = CF124[col[i]] + CF125[col[i]] + CF126[col[i]]
        except:
            pass\


    tempCF = {
        'CF11' : CF11,
        'CF12' : CF12 ,
        'CF13' : CF13,
        'CF14' : CF14,
        'CF15' : CF15,
        'CF16' : CF16,
        'CF17' : CF17,
        'CF18' : CF18,
        'CF19' : CF19,
        'CF20' : CF20,
        'CF21':CF21,
        'CF22':CF22,
        'CF23':CF23,
        'CF24': CF24,
        'CF27': CF27,
        'CF28':CF28,
        'CF29': CF29,
        'CF30': CF30 ,
        'CF31': CF31,
        'CF34': CF34,
        'CF35':CF35,
        'CF36':CF36,
        'CF37':CF37,
        'CF38': CF38,
        'CF39':CF39,
        'CF40': CF40,
        'CF41':CF41,
        'CF43': CF43,
        'CF44':CF44,
        'CF45':CF45,
        'CF50':CF50,
        'CF51': CF51,
        'CF52':CF52,
        'CF53':CF53,
        'CF56':CF56,
        'CF57':CF57,
        'CF58':CF58,
        'CF59':CF59,
        'CF60' : CF60,
        'CF61': CF61,
        'CF62':CF62,
        'CF63':CF63,
        'CF64':CF64,
        'CF67': CF67,
        'CF68':CF68,
        'CF69':CF69,
        'CF74':CF74,
        'CF75':CF75,
        'CF76':CF76,
        'CF77':CF77,
        'CF80':CF80,
        'CF81':CF81,
        'CF82':CF82,
        'CF83':CF83,
        'CF84':CF84,
        'CF85':CF85,
        'CF86':CF86,
        'CF89':CF89,
        'CF90':CF90,
        'CF91':CF91,
        'CF92':CF92,
        'CF93':CF93,
        'CF94':CF94,
        'CF95':CF95,
        'CF96':CF96,
        'CF97':CF97,
        'CF100':CF100,
        'CF101':CF101,
        'CF102':CF102,
        'CF103':CF103,
        'CF104':CF104,
        'CF105':CF105,
        'CF106':CF106,
        'CF107':CF107,
        'CF108':CF108,
        'CF109':CF109,
        'CF110':CF110,
        'CF111':CF111,
        'CF112':CF112,
        'CF113':CF113,
        'CF114':CF114,
        'CF115':CF115,
        'CF116':CF116,
        'CF120':CF120,
        'CF121':CF121,
        'CF122':CF122,
        'CF124':CF124,
        'CF125':CF125,
        'CF126':CF126,
        'CF127':CF127
             }

    dfCF = pd.DataFrame(tempCF)
    dfCFT = dfCF.transpose()
    return dfCFT


def Build_RA(f,cf):
    RA10 = Series(index=col)
    RA11 = Series(index=col)
    RA12 = Series(index=col)
    RA13 = Series(index=col)
    RA14 = Series(index=col)
    RA15 = Series(index=col)
    RA19 = Series(index=col)
    RA20 = Series(index=col)
    RA21 = Series(index=col)
    RA24 = Series(index=col)
    RA25 = Series(index=col)
    RA26 = Series(index=col)
    RA27 = Series(index=col)
    RA30 = Series(index=col)
    RA31 = Series(index=col)
    RA32 = Series(index=col)
    RA33 = Series(index=col)
    RA34 = Series(index=col)
    RA35 = Series(index=col)
    RA36 = Series(index=col)
    RA40 = Series(index=col)
    RA41 = Series(index=col)
    RA42 = Series(index=col)
    RA43 = Series(index=col)
    RA44 = Series(index=col)
    RA47 = Series(index=col)
    RA48 = Series(index=col)
    RA49 = Series(index=col)
    RA50 = Series(index=col)
    RA51 = Series(index=col)
    RA52 = Series(index=col)
    RA56 = Series(index=col)
    RA57 = Series(index=col)
    RA58 = Series(index=col)
    RA62 = Series(index=col)
    RA63 = Series(index=col)
    RA64 = Series(index=col)
    RA65 = Series(index=col)

    for i in range(0, 19):
        try:
            RA10[col[i+1]] = f.at['F12', col[i + 1]] / f.at['F12', col[i]] - 1

            if (f.at['F42', col[i]]) <= 0:
                RA11[col[i+1]] = 'N/A'
            else:
                RA11[col[i+1]] = f.at['F42', col[i + 1]] / f.at['F42', col[i]] - 1

            if (f.at['F57', col[i]]) <= 0:
                RA12[col[i + 1]] = 'N/A'
            else:
                RA12[col[i + 1]] = f.at['F57', col[i + 1]] / f.at['F57', col[i]] - 1

            if (f.at['F29', col[i]]) <= 0:
                RA13[col[i + 1]] = 'N/A'
            else:
                RA13[col[i + 1]] = f.at['F29', col[i + 1]] / f.at['F29', col[i]] - 1
        #14,15는 D부터 시작/다른 루프에서 돌림 15는 19참조하므로 아래쪽에서.

            RA19[col[i+1]] = f.at['F73', col[i + 1]] / ((f.at['F77', col[i]] + f.at['F77', col[i+1]])/2)

            if (f.at['F22', col[i + 1]]) == 0:
                RA20[col[i+1]] = (f.at['F29', col[i+1]] - f.at['F26', col[i+1]] - f.at['F24', col[i+1]]) /  ((f.at['F77', col[i]] + f.at['F77', col[i+1]])/2)
            else:
                RA20[col[i + 1]] = (f.at['F29', col[i + 1]] - f.at['F26', col[i + 1]] - f.at['F24', col[i + 1]] - (f.at['F22', col[i + 1]] + f.at['F23', col[i + 1]])/ f.at['F22',col[i+1]] * f.at['F21', col[i + 1]]) / ((f.at['F77', col[i]] + f.at['F77', col[i+1]])/2)

            RA21[col[i + 1]] = f.at['F71', col[i + 1]] / ((f.at['F75', col[i]] + f.at['F75', col[i + 1]]) / 2)

            RA24[col[i+1]] = f.at['F29',col[i+1]]/ f.at['F12',col[i+1]]

            RA25[col[i+1]] = f.at['F12', col[i+1]] / ((f.at['F42', col[i]] + f.at['F42', col[i+1]])/2)

            RA26[col[i+1]] = (f.at['F58', col[i]] + f.at['F58', col[i+1]]) / (f.at['F57', col[i]] + f.at['F57', col[i + 1]])

            RA27[col[i+1]] = RA24[col[i+1]] * RA25[col[i+1]] * RA26[col[i]]

            RA30[col[i+1]] = f.at['F71', col[i+1]] / f.at['F12', col[i+1]]

            RA31[col[i + 1]] = f.at['F12', col[i + 1]] / ((f.at['F75', col[i]] + f.at['F75', col[i + 1]]) / 2)

            RA32[col[i+1]] = RA30[col[i+1]] * RA31[col[i+1]]

            RA33[col[i + 1]] = - f.at['F72', col[i + 1]] / ((f.at['F76', col[i]] + f.at['F76', col[i + 1]]) / 2)

            RA34[col[i+1]] = RA32[col[i+1]] - RA33[col[i+1]]

            RA35[col[i+1]] = (f.at['F76', col[i]] + f.at['F76', col[i+1]]) / (f.at['F77', col[i]] + f.at['F77', col[i + 1]])

            RA36[col[i+1]] = RA32[col[i+1]] + RA35[col[i+1]] * RA34[col[i+1]]

            RA40[col[i+1]] = f.at['F14', col[i+1]] / f.at['F12', col[i+1]]

            RA41[col[i+1]] = f.at['F17', col[i+1]] / f.at['F12', col[i+1]]

            RA42[col[i+1]] = f.at['F19', col[i+1]] / f.at['F12', col[i+1]]

            if (f.at['F22', col[i+1]]) == 0:
                RA43[col[i+1]] = (f.at['F71', col[i+1]] - f.at['F21', col[i+1]] - f.at['F26', col[i+1]] - f.at['F24', col[i+1]])/f.at['F12', col[i+1]]
            else:
                RA43[col[i+1]] = (f.at['F71', col[i+1]] - f.at['F21', col[i+1]] *(1- (- f.at['F23', col[i+1]]/f.at['F22', col[i+1]]) ) - f.at['F26', col[i+1]] - f.at['F24', col[i+1]]) / \
                               f.at['F12', col[i+1]]

            RA44[col[i+1]] = f.at['F71', col[i+1]] / f.at['F12', col[i+1]]

            RA47[col[i + 1]] = f.at['F12', col[i + 1]] / ((f.at['F75', col[i+1]] + f.at['F75', col[i]]) / 2)

            RA48[col[i + 1]] = f.at['F12', col[i + 1]] / ((f.at['F37', col[i]] - f.at['F48', col[i]] + f.at['F44', col[i]] + f.at['F37', col[i+1]] - f.at['F48', col[i+1]] + f.at['F44', col[i+1]]) / 2)

            RA49[col[i + 1]] = 365 * (((f.at['F34', col[i]] + f.at['F34', col[i+1]])/2)/f.at['F12', col[i+1]])

            RA50[col[i + 1]] = - 365 * (((f.at['F35', col[i]] + f.at['F35', col[i+1]])/2)/f.at['F13', col[i+1]])

            RA51[col[i + 1]] = 365 * (((f.at['F45', col[i]] + f.at['F45', col[i+1]])/2)/(-f.at['F13', col[i+1]] + f.at['F35', col[i+1]] - f.at['F35', col[i]]))

            RA52[col[i + 1]] = f.at['F12', col[i + 1]] / ((f.at['F38', col[i]] + f.at['F38', col[i + 1]]) / 2)

            RA56[col[i+1]] = (f.at['F44', col[i+1]] + f.at['F49', col[i+1]]) / f.at['F57', col[i+1]]

            RA57[col[i+1]] = (f.at['F29', col[i+1]] - f.at['F18', col[i+1]] + f.at['F51', col[i+1]] - f.at['F51', col[i]] + f.at['F50', col[i+1]] - f.at['F50', col[i]] - f.at['F27', col[i+1]] - f.at['F28', col[i+1]]) / \
                            (( f.at['F44', col[i+1]] + f.at['F49', col[i+1]] + f.at['F44', col[i]] + f.at['F49', col[i]])/2)

            RA58[col[i+1]] = ((f.at['F29', col[i+1]] - f.at['F18', col[i+1]] + f.at['F51', col[i+1]] - f.at['F51', col[i]] + f.at['F50', col[i+1]] - f.at['F50', col[i]] - f.at['F27', col[i+1]] - f.at['F28', col[i+1]])  \
                             - (f.at['F34', col[i+1]] - f.at['F34', col[i]]) - (f.at['F35', col[i+1]] - f.at['F35', col[i]] ) \
                              - (f.at['F36', col[i + 1]] - f.at['F36', col[i]]) + (f.at['F45', col[i+1]] - f.at['F45', col[i]] ) \
                              + (f.at['F46', col[i + 1]] - f.at['F46', col[i]]) + (f.at['F47', col[i+1]] - f.at['F47', col[i]] ) ) \
                            / (( f.at['F44', col[i+1]] + f.at['F49', col[i+1]] + f.at['F44', col[i]] + f.at['F49', col[i]])/2)

            RA62[col[i+1]] = f.at['F37', col[i+1]] / f.at['F48', col[i+1]]
            RA63[col[i+1]] = (f.at['F33', col[i+1]] + f.at['F34', col[i+1]]) / f.at['F48', col[i+1]]
            RA64[col[i+1]] = -f.at['F19', col[i+1]] / f.at['F20', col[i+1]]
            RA65[col[i+1]] = -f.at['F17', col[i+1]] / f.at['F20', col[i+1]]
        except:
            pass

    for j in range(0,18):
        try:
            # RA14, 15 는 여기서
            if cf.at['CF116', col[j]] <=0:
                RA14[col[j+2]] = 'N/A'
            else:
                RA14[col[j + 2]] = cf.at['CF116', col[j+1]] / cf.at['CF116', col[j]]-1

            if f.at['F29',col[j+2]] == 0:
                RA15[col[j+2]] = 0
            else:
                RA15[col[j + 2]] = RA19[col[j+2]] *(1 - (- f.at['F64', col[j+2]]/f.at['F29', col[j+2]]))
        except:
            pass
    tempRA = { 'RA10':RA10, 'RA11':RA11,'RA12':RA12,'RA13':RA13,'RA14':RA14,'RA15':RA15,'RA19':RA19, 'RA20':RA20, 'RA21':RA21,\
             'RA24':RA24,'RA25':RA25,'RA26':RA26,'RA27':RA27,'RA30':RA30,'RA31':RA31,'RA32':RA32,'RA33':RA33,'RA34':RA34,'RA35':RA35,\
            'RA36':RA36,'RA40':RA40,'RA41':RA41,'RA42':RA42,'RA43':RA43,'RA44':RA44,'RA47':RA47,'RA48':RA48,'RA49':RA49,'RA50':RA50,\
            'RA51':RA51,'RA52':RA52,'RA56':RA56,'RA57':RA57,'RA58':RA58,'RA62':RA62,'RA63':RA63, 'RA64':RA64,'RA65':RA65
            }

    dfRA = pd.DataFrame(tempRA)
    dfRAr = dfRA.round(4)
    dfRAT = dfRAr.transpose()
    dfRATN = dfRAT.replace([np.inf, -np.inf], np.nan)
    return dfRATN


def Build_CR(f,cf):
    CR11 = Series(index=col)
    CR12 = Series(index=col)
    CR13 = Series(index=col)
    CR17 = Series(index=col)
    CR18 = Series(index=col)
    CR19 = Series(index=col)
    CR20 = Series(index=col)
    CR23 = Series(index=col)
    CR24 = Series(index=col)
    CR25 = Series(index=col)
    CR26 = Series(index=col)
    CR27 = Series(index=col)
    CR28 = Series(index=col)
    CR29 = Series(index=col)
    CR30 = Series(index=col)
    CR31 = Series(index=col)
    CR32 = Series(index=col)
    CR33 = Series(index=col)
    CR34 = Series(index=col)
    CR35 = Series(index=col)

    for i in range(0,19):
        try:
            CR11[col[i+1]] = (f.at['F44', col[i+1]] + f.at['F49', col[i+1]]) / f.at['F57', col[i+1]]
            CR12[col[i+1]] = cf.at['CF17',col[i]]/((f.at['F44', col[i+1]] + f.at['F49', col[i+1]] + f.at['F44', col[i]] + f.at['F49', col[i]])/2)
            CR13[col[i+1]] = cf.at['CF24', col[i]] / ((f.at['F44', col[i + 1]] + f.at['F49', col[i + 1]] + f.at['F44', col[i]] + f.at['F49', col[i]]) / 2)
            CR17[col[i+1]] = f.at['F37', col[i+1]] / f.at['F48', col[i+1]]
            CR18[col[i+1]] = (f.at['F33', col[i+1]] + f.at['F34', col[i+1]]) / f.at['F48', col[i+1]]
            CR19[col[i+1]] = -f.at['F19', col[i+1]] / f.at['F20', col[i+1]]
            CR20[col[i+1]] = -f.at['F17', col[i+1]] / f.at['F20', col[i+1]]
            CR23[col[i+1]] = f.at['F25', col[i+1]] / f.at['F42', col[i+1]]
            CR25[col[i+1]] = f.at['F52', col[i+1]] / f.at['F42', col[i+1]]
            CR27[col[i+1]] = CR18[col[i+1]]
            CR29[col[i+1]] = CR19[col[i+1]]
            CR31[col[i+1]] = -365*f.at['F35', col[i+1]] / f.at['F13', col[i+1]]
            CR33[col[i+1]] = f.at['F12', col[i+1]] / f.at['F12', col[i]] -1

            if CR23[col[i+1]] <  -0.448:
                CR24[col[i+1]] = 0.083
            elif CR23[col[i+1]] < -0.161:
                CR24[col[i+1]] = -0.08
            elif CR23[col[i+1]] < -0.049:
                CR24[col[i+1]] = 0.072
            elif CR23[col[i+1]] < 0.001:
                CR24[col[i+1]] = 0.055
            elif CR23[col[i+1]] < 0.022:
                CR24[col[i+1]] = 0.045
            elif CR23[col[i+1]] < 0.038:
                CR24[col[i+1]] = 0.030
            elif CR23[col[i+1]] < 0.056:
                CR24[col[i+1]] = 0.025
            elif CR23[col[i+1]] < 0.079:
                CR24[col[i+1]] = 0.020
            else:
                CR24[col[i+1]] = 0.020

            if CR25[col[i+1]] < 0.175:
                CR26[col[i+1]] = 0.02
            elif CR25[col[i+1]] < 0.283:
                CR26[col[i+1]] = 0.022
            elif CR25[col[i+1]] < 0.385:
                CR26[col[i+1]] = 0.025
            elif CR25[col[i+1]] < 0.476:
                CR26[col[i+1]] = 0.030
            elif CR25[col[i+1]] < 0.558:
                CR26[col[i+1]] = 0.035
            elif CR25[col[i+1]] < 0.633:
                CR26[col[i+1]] = 0.045
            elif CR25[col[i+1]] < 0.709:
                CR26[col[i+1]] = 0.055
            elif CR25[col[i+1]] < 0.816:
                CR26[col[i+1]] = 0.070
            elif CR25[col[i+1]] < 1.030:
                CR26[col[i+1]] = 0.090
            else:
                CR26[col[i+1]] = 0.10


            if CR27[col[i+1]] < 0.382:
                CR28[col[i+1]] = 0.09
            elif CR27[col[i+1]] < 0.522:
                CR28[col[i+1]] = 0.065
            elif CR27[col[i+1]] < 0.809:
                CR28[col[i+1]] = 0.05
            elif CR27[col[i+1]] < 0.994:
                CR28[col[i+1]] = 0.042
            elif CR27[col[i+1]] < 1.2:
                CR28[col[i+1]] = 0.04
            elif CR27[col[i+1]] < 1.470:
                CR28[col[i+1]] = 0.035
            elif CR27[col[i+1]] < 1.890:
                CR28[col[i+1]] = 0.025
            elif CR27[col[i+1]] < 2.530:
                CR28[col[i+1]] = 0.020
            elif CR27[col[i+1]] < 4.550:
                CR28[col[i+1]] = 0.015
            else:
                CR28[col[i+1]] = 0.010


            if CR29[col[i+1]] < -17.4:
                CR30[col[i+1]] = 0.075
            elif CR29[col[i+1]] < -3.6:
                CR30[col[i+1]] = 0.09
            elif CR29[col[i+1]] < -0.139:
                CR30[col[i+1]] = 0.085
            elif CR29[col[i+1]] < 1.25:
                CR30[col[i+1]] = 0.07
            elif CR29[col[i+1]] < 2.21:
                CR30[col[i+1]] = 0.050
            elif CR29[col[i+1]] < 3.387:
                CR30[col[i+1]] = 0.03
            elif CR29[col[i+1]] < 5.21:
                CR30[col[i+1]] = 0.021
            elif CR29[col[i+1]] < 9.16:
                CR30[col[i+1]] = 0.018
            elif CR29[col[i+1]] < 23.8:
                CR30[col[i+1]] = 0.015
            else:
                CR30[col[i+1]] = 0.010


            if CR31[col[i+1]] < 0.365:
                CR32[col[i+1]] = 0.03
            elif CR31[col[i+1]] < 2.56:
                CR32[col[i+1]] = 0.035
            elif CR31[col[i+1]] < 14.24:
                CR32[col[i+1]] = 0.039
            elif CR31[col[i+1]] < 31.39:
                CR32[col[i+1]] = 0.040
            elif CR31[col[i+1]] < 50:
                CR32[col[i+1]] = 0.042
            elif CR31[col[i+1]] < 70.08:
                CR32[col[i+1]] = 0.045
            elif CR31[col[i+1]] < 91.98:
                CR32[col[i+1]] = 0.049
            elif CR31[col[i+1]] < 121.54:
                CR32[col[i+1]] = 0.055
            elif CR31[col[i+1]] < 174.47:
                CR32[col[i+1]] = 0.06
            else:
                CR32[col[i+1]] = 0.070

            CR34[col[0]] = 0.042
            if CR33[col[i+1]] < -0.203:
                CR34[col[i+1]] = 0.069
            elif CR33[col[i+1]] < -0.063:
                CR34[col[i+1]] = 0.055
            elif CR33[col[i+1]] < 0.003:
                CR34[col[i+1]] = 0.042
            elif CR33[col[i+1]] < 0.05:
                CR34[col[i+1]] = 0.032
            elif CR33[col[i+1]] < 0.098:
                CR34[col[i+1]] = 0.030
            elif CR33[col[i+1]] < 0.158:
                CR34[col[i+1]] = 0.030
            elif CR33[col[i+1]] < 0.244:
                CR34[col[i+1]] = 0.032
            elif CR33[col[i+1]] < 0.398:
                CR34[col[i+1]] = 0.042
            elif CR33[col[i+1]] < 0.797:
                CR34[col[i+1]] = 0.050
            else:
                CR34[col[i+1]] = 0.065

            CR35[col[i+1]] = (CR24[col[i+1]]+CR26[col[i+1]]+CR28[col[i+1]]+CR30[col[i+1]]+CR32[col[i+1]]+CR34[col[i+1]])/6
        except:
            pass
    tempCR = {
        'CR11':CR11,
        'CR12':CR12,
        'CR13': CR13,
        'CR17': CR17,
        'CR18': CR18,
        'CR19': CR19,
        'CR20': CR20,
        'CR23': CR23,
        'CR24': CR24,
        'CR25': CR25,
        'CR26': CR26,
        'CR27': CR27,
        'CR28': CR28,
        'CR29': CR29,
        'CR30':CR30,
        'CR31': CR31,
        'CR32':CR32,
        'CR33': CR33,
        'CR34': CR34,
        'CR35': CR35
        }

    dfCR = pd.DataFrame(tempCR)
    dfCRT = dfCR.transpose()
    return dfCRT


if __name__ == "__main__":
    col = ['B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U']
    coll = ['B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U']

    col_small = ['C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U']

    con = sqlite3.connect('C:/a/ext19yrs_eval.db')
    #nowtime = datetime.datetime.now().strftime('%Y%m%d%H%M')
    yrs = DBlist('C:/a/ext19yrs_eval.db')  # 리스트 호출~

    fileloc = 'C:/a/ext19yrs_eval.db'

    e = dataload(fileloc)
    company_screen = e.transpose(2,0,1)

    y2018 = company_screen.major_xs('2018')    #한해 자료 다 뽑을때
    PPL = y2018.index
    temp = e.major_xs(PPL[2]) #한회사의 20년치 재무제표 다 뽑음.

    panel_book = {}

    for i,j in enumerate(PPL):
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
        CF = CF.drop("U",axis = 1)
        CF.columns = col_small
        Wholedata = pd.concat([wholeFS, CF, RA, CR], axis=0)
        Wholedata = Wholedata.drop(["B"], axis=1)
        Wholedata = Wholedata.fillna(0)
        Wholedata.columns = yrs[1:]
        temp_data = {j : Wholedata}
        panel_book.update(temp_data)

    big_one = pd.Panel.from_dict(panel_book, intersect=False, orient='items', dtype=None)
    new_one = big_one.transpose(2, 0, 1)
    conn = sqlite3.connect('C:/a/ext19yrs_evalfull.db')

    df00a =new_one['2000']
    df01a = new_one['2001']
    df02a = new_one['2002']
    df03a = new_one['2003']
    df04a = new_one['2004']
    df05a = new_one['2005']
    df06a = new_one['2006']
    df07a = new_one['2007']
    df08a = new_one['2008']
    df09a = new_one['2009']
    df10a = new_one['2010']
    df11a = new_one['2011']
    df12a = new_one['2012']
    df13a = new_one['2013']
    df14a = new_one['2014']
    df15a = new_one['2015']
    df16a = new_one['2016']
    df17a = new_one['2017']
    df18a = new_one['2018']

    df00a.to_sql('2000', conn, if_exists='replace')
    df01a.to_sql('2001', conn, if_exists='replace')
    df02a.to_sql('2002', conn, if_exists='replace')
    df03a.to_sql('2003', conn, if_exists='replace')
    df04a.to_sql('2004', conn, if_exists='replace')
    df05a.to_sql('2005', conn, if_exists='replace')
    df06a.to_sql('2006', conn, if_exists='replace')
    df07a.to_sql('2007', conn, if_exists='replace')
    df08a.to_sql('2008', conn, if_exists='replace')
    df09a.to_sql('2009', conn, if_exists='replace')
    df10a.to_sql('2010', conn, if_exists='replace')
    df11a.to_sql('2011', conn, if_exists='replace')
    df12a.to_sql('2012', conn, if_exists='replace')
    df13a.to_sql('2013', conn, if_exists='replace')
    df14a.to_sql('2014', conn, if_exists='replace')
    df15a.to_sql('2015', conn, if_exists='replace')
    df16a.to_sql('2016', conn, if_exists='replace')
    df17a.to_sql('2017', conn, if_exists='replace')
    df18a.to_sql('2018', conn, if_exists='replace')





