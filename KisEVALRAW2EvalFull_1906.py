    #12월3일현재 잘 작동하는 공식버전. kis 로부터 받은 evalrawnonpub1201byname를 받아서 변환.
#
# 싫행성공/ 19-6-03
#6년치를 변환해 5년치를 완벽하게 보여주는 버전
import requests
import pandas as pd
from pandas import Series, DataFrame
import json
import sqlite3
from pandas import ExcelWriter
import time
import numpy as np


Terminal = 0.03  #Terminal sales growth - 산업에 따라 다르고 업체에따라 다르나 7%는 넘기지 말도록 /가장 중요한 Assumption
col = ['B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q','R']

collen = len(col)


#받아올 데이터 베이스 지정 및 DB의 종목 리스트 작성 / 리스트 : PL -=-> 메인실행으로 옮김

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

# 받아올 데이터 트리밍 작업
def import_com(com_code, fileloc):
    con = sqlite3.connect(fileloc)
    df = pd.read_sql("SELECT * FROM '%s'" % com_code, con, index_col='index')
    df1 = df[['2014', '2015', '2016', '2017', '2018']]
    return df1


def reindex(data):
# 항목매칭작업
    temp = {'01 Sales': 'sales',
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
def build_FS(data1):
    #data2 = data1.drop('2017/12(E)', axis=1)
    data = data1.rename(columns={'2014': 'B', '2015': 'C', '2016': 'D', '2017':'E', '2018': 'F'})  # 인덱스 변경

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
    coll = ['B','C','D','E','F']

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

    F62 = Series(index=coll)
    F63 = Series(index=coll)
    F64 = Series(index=coll)
    F65 = Series(index=coll)
    F66 = Series(index=coll)

    for i in range (1,5):
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

def reindex(data):
# 항목매칭작업
    temp = {'01 Sales': 'sales',
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


def ForecastingAss(df):

    collen = len(col)

    A14 = Series(index=col)
    A15 = Series(index=col)
    A16 = Series(index=col)
    A17 = Series(index=col)
    A18 = Series(index=col)
    A19 = Series(index=col)
    A20 = Series(index=col)
    A21 = Series(index=col)
    A22 = Series(index=col)
    A23 = Series(index=col)
    A24 = Series(index=col)
    A25 = Series(index=col)

    A14['Q'] = Terminal


    for i in range(1, 5):                      #Sales Growth
        A14[col[i]] = df.at['F12', col[i]] / df.at['F12', col[i - 1]] - 1
    for j in range(5,15):
        A14[col[j]] = A14[col[j-1]] - ( A14[col[j-1]] - A14['Q']) / (16-j)
    A14['R']=A14['Q']

    for i in range(0, 5):  # COGS/Sales
        A15[col[i]] = -(df.at['F13', col[i]] / df.at['F12', col[i]])
    A15['Q'] = A15['F']                             #set Terminal value as the last fiscal year's data
    for j in range(5, 16):
        A15[col[j]] = A15[col[j - 1]] - (A15[col[j - 1]] - A15['Q']) / (16 - j)
    A15['R'] = A15['Q']

    for i in range(0, 5):  # R&D
        A16[col[i]] = -(df.at['F15', col[i]] / df.at['F12', col[i]])
    A16['Q'] = A16['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
        A16[col[j]] = A16[col[j - 1]] - (A16[col[j - 1]] - A16['Q']) / (16 - j)
    A16['R'] = A16['Q']

    for i in range(0, 5):  # SGA/sales
        A17[col[i]] = -(df.at['F16', col[i]] / df.at['F12', col[i]])
    A17['Q'] = A17['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
        A17[col[j]] = A17[col[j - 1]] - (A17[col[j - 1]] - A17['Q']) / (16 - j)
    A17['R'] = A17['Q']

    for i in range(1, 5):  # Dep%Amort/avge PP&E and Intangible
        if (df.at['F38', col[i-1]]+ df.at['F38', col[i]]+ df.at['F40', col[i-1]]+ df.at['F40', col[i]]) == 0:
            A18[col[i]] = 0
        else:
            A18[col[i]] = - ( df.at['F18', col[i]]) / (( df.at['F38', col[i-1]] +  df.at['F38', col[i]]+
                                                      df.at['F40', col[i-1]]+ df.at['F40', col[i]] )/2)
    A18['Q'] = A18['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
        A18[col[j]] = A18[col[j - 1]] - (A18[col[j - 1]] - A18['Q']) / (16 - j)
    A18['R'] = A18['Q']

    for i in range(1, 5):  # Net Interest Expense/Avge Net Debt
        if (df.at['F44', col[i-1]]+ df.at['F44', col[i]]+ df.at['F49', col[i-1]]+ df.at['F49', col[i]]) == 0:
            A19[col[i]] = 0
        else:
            A19[col[i]] = - ( df.at['F20', col[i]]) / (( df.at['F44', col[i-1]] +  df.at['F44', col[i]]+
                                                      df.at['F49', col[i-1]]+ df.at['F49', col[i]] )/2)
    A19['Q'] = A19['F']  # set Terminal value as the last fiscal year's data
    A19['R'] = A19['Q']
    for j in range(5, 16):
        A19[col[j]] = A19[col[j - 1]] - (A19[col[j - 1]] - A19['Q']) / (16 - j)

    for i in range(0, 5):  # non operating income/sales
        A20[col[i]] = df.at['F21', col[i]] / df.at['F12', col[i]]
    A20['Q'] = A20['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
            A20[col[j]] = A20[col[j - 1]] - (A20[col[j - 1]] - A20['Q']) / (16 - j)
    A20['R'] = A20['Q']

    for i in range(0, 5):  # Effective Tax Rate
        if df.at['F22', col[i]] == 0:
            A21[col[i]] = 0
        else:
            A21[col[i]] = -(df.at['F23', col[i]] / df.at['F22', col[i]])
    A21['Q'] = A21['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
        A21[col[j]] = A21[col[j - 1]] - (A21[col[j - 1]] - A21['Q']) / (16 - j)
    A21['R'] = A21['Q']

    for i in range(0, 5):  # Minority Interest/After Tax Income
        if df.at['F22', col[i]]+df.at['F23', col[i]] == 0:
            A22[col[i]] = 0
        else:
            A22[col[i]] = -(df.at['F27', col[i]] / (df.at['F22', col[i]]+df.at['F23', col[i]]))
    A22['Q'] = A22['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
        A22[col[j]] = A22[col[j - 1]] - (A22[col[j - 1]] - A22['Q']) / (16 - j)
    A22['R'] = A22['Q']

    for i in range(0, 5):  # Other Income/Sales
        A23[col[i]] = df.at['F24', col[i]] / df.at['F12', col[i]]
    A23['G'] = 0
    for j in range(6, 16):
        A23[col[j]] = A23[col[j - 1]]
    A23['R'] = A23['Q']

    for i in range(0, 5):  # Ext. Items & Disc. Ops./Sales
        A24[col[i]] = df.at['F26', col[i]] / df.at['F12', col[i]]
    A24['G'] = 0
    for j in range(6, 16):
        A24[col[j]] = A24[col[j - 1]]
    A24['R'] = A24['Q']

    for i in range(1, 5):  # Pref. Dividends/Avge Pref. Stock
        if ((df.at['F54', col[i]]+ df.at['F54', col[i-1]])/2) == 0:
            A25[col[i]] = 0
        else:
            A25[col[i]] = -  df.at['F28', col[i]] / (( df.at['F54', col[i]] +  df.at['F54', col[i-1]])/2)
    A25['Q'] = A25['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
        A25[col[j]] = A25[col[j - 1]] - (A25[col[j - 1]] - A25['Q']) / (16 - j)
    A25['R'] = A25['Q']

#Balance Sheet Assumptions
#Working Capital Assumptions

    A29 = Series(index=col)
    A30 = Series(index=col)
    A31 = Series(index=col)
    A32 = Series(index=col)
    A33 = Series(index=col)
    A34 = Series(index=col)
    A35 = Series(index=col)
    A37 = Series(index=col)
    A38 = Series(index=col)
    A39 = Series(index=col)
    A40 = Series(index=col)
    A42 = Series(index=col)
    A43 = Series(index=col)
    A45 = Series(index=col)
    A46 = Series(index=col)
    A47 = Series(index=col)
    A48 = Series(index=col)
    A49 = Series(index=col)

    for i in range(0, 5):  # Ending Operating Cash/Sales
        A29[col[i]] = df.at['F33', col[i]] / df.at['F12', col[i]]
    A29['Q'] = A29['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
            A29[col[j]] = A29[col[j - 1]] - (A29[col[j - 1]] - A29['Q']) / (16 - j)
    A29['R'] = A29['Q']

    for i in range(0, 5):  # Ending Receivables/Sales
        A30[col[i]] = df.at['F34', col[i]] / df.at['F12', col[i]]
    A30['Q'] = A30['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
            A30[col[j]] = A30[col[j - 1]] - (A30[col[j - 1]] - A30['Q']) / (16 - j)
    A30['R'] = A30['Q']

    for i in range(0, 5):  # Ending Inventories/COGS
        if (df.at['F13', col[i]] ) == 0:
            A31[col[i]] = 0
        else:
            A31[col[i]] = - (( df.at['F35', col[i]]) / ( df.at['F13', col[i]]))
    A31['Q'] = A31['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
        A31[col[j]] = A31[col[j - 1]] - (A31[col[j - 1]] - A31['Q']) / (16 - j)
    A31['R'] = A31['Q']

    for i in range(0, 5):  # Ending Other Current Assets/Sales
        A32[col[i]] = df.at['F36', col[i]] / df.at['F12', col[i]]
    A32['Q'] = A32['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
            A32[col[j]] = A32[col[j - 1]] - (A32[col[j - 1]] - A32['Q']) / (16 - j)
    A32['R'] = A32['Q']

    for i in range(0, 5):  # Ending Accounts Payable/COGS
        if (df.at['F13', col[i]] ) == 0:
            A33[col[i]] = 0
        else:
            A33[col[i]] = - (( df.at['F45', col[i]]) / ( df.at['F13', col[i]]))
    A33['Q'] = A33['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
        A33[col[j]] = A33[col[j - 1]] - (A33[col[j - 1]] - A33['Q']) / (16 - j)
    A33['R'] = A33['Q']

    for i in range(0, 5):  # Ending Other Current Assets/Sales
        A34[col[i]] = df.at['F46', col[i]] / df.at['F12', col[i]]
    A34['Q'] = A34['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
            A34[col[j]] = A34[col[j - 1]] - (A34[col[j - 1]] - A34['Q']) / (16 - j)
    A34['R'] = A34['Q']

    for i in range(0, 5):  # Ending Other Current Liabs/Sales
        A35[col[i]] = df.at['F47', col[i]] / df.at['F12', col[i]]
    A35['Q'] = A35['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
            A35[col[j]] = A35[col[j - 1]] - (A35[col[j - 1]] - A35['Q']) / (16 - j)
    A35['R'] = A35['Q']
#Other Operating Asset Assumptions

    for i in range(0, 5):  # Ending Net PP&E/Sales
        A37[col[i]] = df.at['F38', col[i]] / df.at['F12', col[i]]
    A37['Q'] = A37['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
            A37[col[j]] = A37[col[j - 1]] - (A37[col[j - 1]] - A37['Q']) / (16 - j)
    A37['R'] = A37['Q']

    for i in range(0, 5):  # Ending Investments/Sales
        A38[col[i]] = df.at['F39', col[i]] / df.at['F12', col[i]]
    A38['Q'] = A38['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
            A38[col[j]] = A38[col[j - 1]] - (A38[col[j - 1]] - A38['Q']) / (16 - j)
    A38['R'] = A38['Q']

    for i in range(0, 5):  # Ending Intangibles/Sales
        A39[col[i]] = df.at['F40', col[i]] / df.at['F12', col[i]]
    A39['Q'] = A39['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
            A39[col[j]] = A39[col[j - 1]] - (A39[col[j - 1]] - A39['Q']) / (16 - j)
    A39['R'] = A39['Q']

    for i in range(0, 5):  # Ending Other Assets/Sales
        A40[col[i]] = df.at['F41', col[i]] / df.at['F12', col[i]]
    A40['Q'] = A40['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
            A40[col[j]] = A40[col[j - 1]] - (A40[col[j - 1]] - A40['Q']) / (16 - j)
    A40['R'] = A40['Q']
#Other Operating Liability Assumptions

    for i in range(0, 5):  # Other Liabilities/Sales
        A42[col[i]] = df.at['F50', col[i]] / df.at['F12', col[i]]
    A42['Q'] = A42['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
            A42[col[j]] = A42[col[j - 1]] - (A42[col[j - 1]] - A42['Q']) / (16 - j)
    A42['R'] = A42['Q']

    for i in range(0, 5):  # Deferred Taxes/Sales
        A43[col[i]] = df.at['F51', col[i]] / df.at['F12', col[i]]
    A43['Q'] = A43['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
            A43[col[j]] = A43[col[j - 1]] - (A43[col[j - 1]] - A43['Q']) / (16 - j)
    A43['R'] = A43['Q']
#Financing Assumptions


    for i in range(0, 5):  # Current Debt/Total Assets
        A45[col[i]] = df.at['F44', col[i]] / df.at['F42', col[i]]
    A45['Q'] = A45['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
            A45[col[j]] = A45[col[j - 1]] - (A45[col[j - 1]] - A45['Q']) / (16 - j)
    A45['R'] = A45['Q']

    for i in range(0, 5):  # Long-Term Debt/Total Assets
        A46[col[i]] = df.at['F49', col[i]] / df.at['F42', col[i]]
    A46['Q'] = A46['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
            A46[col[j]] = A46[col[j - 1]] - (A46[col[j - 1]] - A46['Q']) / (16 - j)
    A46['R'] = A46['Q']

    for i in range(0, 5):  # Minority Interest/Total Assets
        A47[col[i]] = df.at['F53', col[i]] / df.at['F42', col[i]]
    A47['Q'] = A47['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
            A47[col[j]] = A47[col[j - 1]] - (A47[col[j - 1]] - A47['Q']) / (16 - j)
    A47['R'] = A47['Q']

    for i in range(0, 5):  # Preferred Stock/Total Assets
        A48[col[i]] = df.at['F54', col[i]] / df.at['F42', col[i]]
    A48['Q'] = A48['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
            A48[col[j]] = A48[col[j - 1]] - (A48[col[j - 1]] - A48['Q']) / (16 - j)
    A48['R'] = A48['Q']

    for i in range(0, 5):  # Dividend Payout Ratio
        if (df.at['F29', col[i]] ) == 0:
            A49[col[i]] = 0
        else:
            A49[col[i]] = - (( df.at['F64', col[i]]) / ( df.at['F29', col[i]]))
    A49['Q'] = A49['F']  # set Terminal value as the last fiscal year's data
    for j in range(5, 16):
        A49[col[j]] = A49[col[j - 1]] - (A49[col[j - 1]] - A49['Q']) / (16 - j)
    A49['R'] = A49['Q']

    temp = {
        'A14':A14,'A15':A15,'A16':A16,'A17':A17,'A18':A18,'A19':A19,'A20':A20,'A21':A21,'A22':A22,'A23':A23,'A24':A24,
               'A25':A25,'A29': A29,'A30':A30,'A31':A31,'A32': A32,'A33': A33,'A34': A34,'A35': A35,'A37': A37,'A38':A38,
               'A39': A39,'A40': A40,'A42': A42 ,'A43': A43,'A45': A45,'A46': A46,'A47': A47,'A48': A48,'A49': A49
        }
    df1 = pd.DataFrame(temp)
    df2 = df1.transpose()
    return df2

def save_xls(list_dfs, xls_path, codes):
    writer = pd.ExcelWriter(xls_path, engine='xlsxwriter')
    for n, m in enumerate(list_dfs):
        list_dfs[m].to_excel(writer, codes[m])
    writer.save()

def build_FSFC(f,a):


    colu = ['F','G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R']

    F12 = Series(index=colu)
    F13 = Series(index=colu)
    F14 = Series(index=colu)
    F15 = Series(index=colu)
    F16 = Series(index=colu)
    F17 = Series(index=colu)
    F18 = Series(index=colu)
    F19 = Series(index=colu)
    F20 = Series(index=colu)
    F21 = Series(index=colu)
    F22 = Series(index=colu)
    F23 = Series(index=colu)
    F24 = Series(index=colu)
    F25 = Series(index=colu)
    F26 = Series(index=colu)
    F27 = Series(index=colu)
    F28 = Series(index=colu)
    F29 = Series(index=colu)
    F33 = Series(index=colu)
    F34 = Series(index=colu)
    F35 = Series(index=colu)
    F36 = Series(index=colu)
    F37 = Series(index=colu)
    F38 = Series(index=colu)
    F39 = Series(index=colu)
    F40 = Series(index=colu)
    F41 = Series(index=colu)
    F42 = Series(index=colu)
    F44 = Series(index=colu)
    F45 = Series(index=colu)
    F46 = Series(index=colu)
    F47 = Series(index=colu)
    F48 = Series(index=colu)
    F49 = Series(index=colu)
    F50 = Series(index=colu)
    F51 = Series(index=colu)
    F52 = Series(index=colu)
    F53 = Series(index=colu)
    F54 = Series(index=colu)
    F55 = Series(index=colu)
    F56 = Series(index=colu)
    F57 = Series(index=colu)
    F58 = Series(index=colu)
    F62 = Series(index=colu)
    F63 = Series(index=colu)
    F64 = Series(index=colu)
    F65 = Series(index=colu)
    F66 = Series(index=colu)

#income statement 12-17까지 먼저 작성.

   #F12[colu[1]] = f.at['F12', colu[0]] * (1 + a.at['A14', colu[1]])
    #for i in range(2,13):
     #   F12[colu[i]] = F12[colu[i-1]] * (1 + a.at['A14', colu[i]])

    for i in range(1,13):
        F12[colu[0]] = f.at['F12', colu[0]]
        F12[colu[i]] = F12[colu[i - 1]] * (1 + a.at['A14', colu[i]])

        F13[colu[i]] = - F12[colu[i]] *  a.at['A15', colu[i]]

        F14[colu[i]] = F12[colu[i]] + F13[colu[i]]

        F15[colu[i]] = - F12[colu[i]] * a.at['A16', colu[i]]

        F16[colu[i]] = - F12[colu[i]] * a.at['A17', colu[i]]

        F17[colu[i]] = F14[colu[i]] + F15[colu[i]] + F16[colu[i]]

#Balance sheet을 먼저 작성해야 에러없음. F33부터 먼저 작성, 55,56,57은 제외


        F33[colu[i]] = F12[colu[i]] * a.at['A29', colu[i]]


        F34[colu[i]] = F12[colu[i]] * a.at['A30', colu[i]]


        F35[colu[i]] = - F13[colu[i]] * a.at['A31', colu[i]]


        F36[colu[i]] = F12[colu[i]] * a.at['A32', colu[i]]


        F37[colu[i]] = F33[colu[i]] + F34[colu[i]] + F35[colu[i]] + F36[colu[i]]


        F38[colu[i]] = F12[colu[i]] * a.at['A37', colu[i]]


        F39[colu[i]] = F12[colu[i]] * a.at['A38', colu[i]]


        F40[colu[i]] = F12[colu[i]] * a.at['A39', colu[i]]


        F41[colu[i]] = F12[colu[i]] * a.at['A40', colu[i]]


        F42[colu[i]] = F37[colu[i]] + F38[colu[i]] + F39[colu[i]] + F40[colu[i]] + F41[colu[i]]


        F44[colu[i]] = F42[colu[i]] * a.at['A45', colu[i]]
        F44[colu[0]] = f.at['F44',colu[0]] #참조하기때문에 밖에서 불러서 넣어줌

        F45[colu[i]] = -F13[colu[i]] * a.at['A33', colu[i]]


        F46[colu[i]] = F12[colu[i]] * a.at['A34', colu[i]]


        F47[colu[i]] = F12[colu[i]] * a.at['A35', colu[i]]


        F48[colu[i]] = F44[colu[i]] + F45[colu[i]] + F46[colu[i]] + F47[colu[i]]


        F49[colu[i]] = F42[colu[i]] * a.at['A46', colu[i]]
        F49[colu[0]] = f.at['F49', colu[0]]            #참조하기때문에 밖에서 불러서 넣어줌

        F50[colu[i]] = F12[colu[i]] * a.at['A42', colu[i]]


        F51[colu[i]] = F12[colu[i]] * a.at['A43', colu[i]]


        F52[colu[i]] = F48[colu[i]] + F49[colu[i]] + F50[colu[i]] + F51[colu[i]]


        F53[colu[i]] = F42[colu[i]] * a.at['A47', colu[i]]


        F54[colu[i]] = F42[colu[i]] * a.at['A48', colu[i]]


        F58[colu[i]] = F42[colu[i]]


        F57[colu[i]] = F58[colu[i]] - F52[colu[i]] - F53[colu[i]] - F54[colu[i]]

#Statement of retained earning부터 먼저 시작

        F38[colu[0]] = f.at['F38',colu[0]]
        F40[colu[0]] = f.at['F40',colu[0]]

        F18[colu[i]] = -((F38[colu[i - 1]] + F40[colu[i - 1]] + F38[colu[i]] + F40[colu[i]]) / 2) * a.at[
                    'A18', colu[i]]

        F19[colu[i]] = F17[colu[i]] + F18[colu[i]]


        F20[colu[i]] = -((F44[colu[i-1]] + F49[colu[i-1]] + F44[colu[i]] + F49[colu[i]])/2) * a.at['A19',colu[i]]


        F21[colu[i]] = F12[colu[i]] * a.at['A20', colu[i]]


        F22[colu[i]] = F19[colu[i]] + F20[colu[i]] + F21[colu[i]]


        F23[colu[i]] = -F22[colu[i]] * a.at['A21', colu[i]]


        F24[colu[i]] = F12[colu[i]] * a.at['A23', colu[i]]


        F25[colu[i]] = F22[colu[i]] + F23[colu[i]] + F24[colu[i]]

        F26[colu[i]] = F12[colu[i]] * a.at['A24', colu[i]]

        F27[colu[i]] = -(F22[colu[i]]+F23[colu[i]]) * a.at['A22', colu[i]]

        F54[colu[0]] = f.at['F54', colu[0]]  # fs에서 먼저 불러와서 집어넣음, 다음 나오는 루프때문에 주의해야함.
        F28[colu[i]] = -((F54[colu[i]] + F54[colu[i-1]])/2) * a.at['A25', colu[i]]

        F29[colu[i]] = F25[colu[i]] + F26[colu[i]] + F27[colu[i]] + F28[colu[i]]

        F56[colu[0]] = f.at['F56', colu[0]]  # fs에서 먼저 불러와서 집어넣음, 다음 나오는 루프때문에 주의해야함.

        F62[colu[i]] = F56[colu[i-1]]

        F63[colu[i]] = F29[colu[i]]

        F64[colu[i]] = -F29[colu[i]] * a.at['A49', colu[i]]

        F65[colu[i]] = 0

        F57[colu[i]] = F58[colu[i]] - F52[colu[i]] - F53[colu[i]] - F54[colu[i]]

        F66[colu[i]] = F62[colu[i]] + F63[colu[i]] + F64[colu[i]] + F65[colu[i]]

        F56[colu[i]] = F66[colu[i]]

        F55[colu[i]] = F57[colu[i]] - F56[colu[i]]

    temp = {
            'F12' : F12,
            'F13' : F13,
            'F14' : F14,
            'F15' : F15,
            'F16' : F16,
            'F17' : F17,
            'F18' : F18,
            'F19' : F19,
            'F20' : F20,
            'F21' : F21,
            'F22' : F22,
            'F23' : F23,
            'F24' : F24,
            'F25' : F25,
            'F26' : F26,
            'F27' : F27,
            'F28' : F28,
            'F29' : F29,
            'F33' : F33,
            'F34' : F34,
            'F35' : F35,
            'F36' : F36,
            'F37' : F37,
            'F38' : F38,
            'F39' : F39,
            'F40' : F40,
            'F41' : F41,
            'F42' : F42,
            'F44' : F44,
            'F45' : F45,
            'F46' : F46,
            'F47' : F47,
            'F48' : F48,
            'F49' : F49,
            'F50' : F50,
            'F51' : F51,
            'F52' : F52,
            'F53' : F53,
            'F54' : F54,
            'F55' : F55,
            'F56' : F56,
            'F57' : F57,
            'F58' : F58,
            'F62' : F62,
            'F63' : F63,
            'F64' : F64,
            'F65' : F65,
            'F66' : F66
            }


    df1 = pd.DataFrame(temp)
    df2 = df1.transpose()
    df3 = df2.drop('F',axis=1)
    return df3


def save_xls(list_dfs, xls_path, codes):
    writer = pd.ExcelWriter(xls_path, engine='xlsxwriter')
    for n, m in enumerate(list_dfs):
        list_dfs[m].to_excel(writer, codes[m])
    writer.save()


def dupont_analysis(data):
        col = ['B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R']
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
        F74 = - data.ix['F23'] / data.ix['F22']
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
    for i in range(0,16):
    #Operating
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


    tempCF = { 'CF11' : CF11, 'CF12' : CF12 , 'CF13' : CF13, 'CF14' : CF14, 'CF15' : CF15, 'CF16' : CF16, 'CF17' : CF17, \
             'CF18' : CF18, 'CF19' : CF19, 'CF20' : CF20, 'CF21':CF21, 'CF22':CF22, 'CF23':CF23, 'CF24': CF24, 'CF27': CF27, \
             'CF28':CF28, 'CF29': CF29, 'CF30': CF30 , 'CF31': CF31, 'CF34': CF34, 'CF35':CF35, 'CF36':CF36, 'CF37':CF37,\
             'CF38': CF38, 'CF39':CF39, 'CF40': CF40, 'CF41':CF41, 'CF43': CF43, 'CF44':CF44,'CF45':CF45,'CF50':CF50, \
             'CF51': CF51,'CF52':CF52,'CF53':CF53,'CF56':CF56,'CF57':CF57,'CF58':CF58,'CF59':CF59,\
             'CF60' : CF60, 'CF61': CF61,'CF62':CF62,'CF63':CF63,'CF64':CF64,'CF67': CF67,'CF68':CF68,'CF69':CF69,'CF74':CF74,\
             'CF75':CF75,'CF76':CF76,'CF77':CF77,'CF80':CF80,'CF81':CF81,'CF82':CF82,'CF83':CF83,'CF84':CF84,'CF85':CF85,\
             'CF86':CF86,'CF89':CF89,'CF90':CF90,'CF91':CF91,'CF92':CF92,'CF93':CF93,'CF94':CF94,'CF95':CF95,'CF96':CF96,\
             'CF97':CF97,'CF100':CF100,'CF101':CF101,'CF102':CF102,'CF103':CF103,'CF104':CF104,'CF105':CF105,'CF106':CF106,\
             'CF107':CF107,'CF108':CF108,'CF109':CF109,'CF110':CF110,'CF111':CF111,'CF112':CF112,'CF113':CF113,'CF114':CF114,\
             'CF115':CF115,'CF116':CF116,'CF120':CF120,'CF121':CF121,'CF122':CF122,'CF124':CF124,'CF125':CF125,'CF126':CF126,'CF127':CF127
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

    for i in range(0, 16):
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

        RA24[col[i]] = f.at['F29',col[i]]/ f.at['F12',col[i]]

        RA25[col[i+1]] = f.at['F12', col[i+1]] / ((f.at['F42', col[i]] + f.at['F42', col[i+1]])/2)

        RA26[col[i+1]] = (f.at['F58', col[i]] + f.at['F58', col[i+1]]) / (f.at['F57', col[i]] + f.at['F57', col[i + 1]])

        RA27[col[i]] = RA24[col[i]] * RA25[col[i]] * RA26[col[i]]

        RA30[col[i]] = f.at['F71', col[i]] / f.at['F12', col[i]]

        RA31[col[i + 1]] = f.at['F12', col[i + 1]] / ((f.at['F75', col[i]] + f.at['F75', col[i + 1]]) / 2)

        RA32[col[i]] = RA30[col[i]] * RA31[col[i]]

        RA33[col[i + 1]] = - f.at['F72', col[i + 1]] / ((f.at['F76', col[i]] + f.at['F76', col[i + 1]]) / 2)

        RA34[col[i]] = RA32[col[i]] - RA33[col[i]]

        RA35[col[i+1]] = (f.at['F76', col[i]] + f.at['F76', col[i+1]]) / (f.at['F77', col[i]] + f.at['F77', col[i + 1]])

        RA36[col[i]] = RA32[col[i]] + RA35[col[i]] * RA34[col[i]]

        RA40[col[i]] = f.at['F14', col[i]] / f.at['F12', col[i]]

        RA41[col[i]] = f.at['F17', col[i]] / f.at['F12', col[i]]

        RA42[col[i]] = f.at['F19', col[i]] / f.at['F12', col[i]]

        if (f.at['F22', col[i]]) == 0:
            RA43[col[i]] = (f.at['F71', col[i]] - f.at['F21', col[i]] - f.at['F26', col[i]] - f.at['F24', col[i]])/f.at['F12', col[i]]
        else:
            RA43[col[i]] = (f.at['F71', col[i]] - f.at['F21', col[i]] *(1- (- f.at['F23', col[i]]/f.at['F22', col[i]]) ) - f.at['F26', col[i]] - f.at['F24', col[i]]) / \
                           f.at['F12', col[i]]

        RA44[col[i]] = f.at['F71', col[i]] / f.at['F12', col[i]]

        RA47[col[i + 1]] = f.at['F12', col[i + 1]] / ((f.at['F75', col[i+1]] + f.at['F75', col[i]]) / 2)

        RA48[col[i + 1]] = f.at['F12', col[i + 1]] / ((f.at['F37', col[i]] - f.at['F48', col[i]] + f.at['F44', col[i]] + f.at['F37', col[i+1]] - f.at['F48', col[i+1]] + f.at['F44', col[i+1]]) / 2)

        RA49[col[i + 1]] = 365 * (((f.at['F34', col[i]] + f.at['F34', col[i+1]])/2)/f.at['F12', col[i+1]])

        RA50[col[i + 1]] = - 365 * (((f.at['F35', col[i]] + f.at['F35', col[i+1]])/2)/f.at['F13', col[i+1]])

        RA51[col[i + 1]] = 365 * (((f.at['F45', col[i]] + f.at['F45', col[i+1]])/2)/(-f.at['F13', col[i+1]] + f.at['F35', col[i+1]] - f.at['F35', col[i]]))

        RA52[col[i + 1]] = f.at['F12', col[i + 1]] / ((f.at['F38', col[i]] + f.at['F38', col[i + 1]]) / 2)

        RA56[col[i]] = (f.at['F44', col[i]] + f.at['F49', col[i]]) / f.at['F57', col[i]]

        RA57[col[i+1]] = (f.at['F29', col[i+1]] - f.at['F18', col[i+1]] + f.at['F51', col[i+1]] - f.at['F51', col[i]] + f.at['F50', col[i+1]] - f.at['F50', col[i]] - f.at['F27', col[i+1]] - f.at['F28', col[i+1]]) / \
                        (( f.at['F44', col[i+1]] + f.at['F49', col[i+1]] + f.at['F44', col[i]] + f.at['F49', col[i]])/2)

        RA58[col[i+1]] = ((f.at['F29', col[i+1]] - f.at['F18', col[i+1]] + f.at['F51', col[i+1]] - f.at['F51', col[i]] + f.at['F50', col[i+1]] - f.at['F50', col[i]] - f.at['F27', col[i+1]] - f.at['F28', col[i+1]])  \
                         - (f.at['F34', col[i+1]] - f.at['F34', col[i]]) - (f.at['F35', col[i+1]] - f.at['F35', col[i]] ) \
                          - (f.at['F36', col[i + 1]] - f.at['F36', col[i]]) + (f.at['F45', col[i+1]] - f.at['F45', col[i]] ) \
                          + (f.at['F46', col[i + 1]] - f.at['F46', col[i]]) + (f.at['F47', col[i+1]] - f.at['F47', col[i]] ) ) \
                        / (( f.at['F44', col[i+1]] + f.at['F49', col[i+1]] + f.at['F44', col[i]] + f.at['F49', col[i]])/2)

        RA62[col[i]] = f.at['F37', col[i]] / f.at['F48', col[i]]
        RA63[col[i]] = (f.at['F33', col[i]] + f.at['F34', col[i]]) / f.at['F48', col[i]]
        RA64[col[i]] = -f.at['F19', col[i]] / f.at['F20', col[i]]
        RA65[col[i]] = -f.at['F17', col[i]] / f.at['F20', col[i]]

    for j in range(0,15):
        # RA14, 15 는 여기서
        if cf.at['CF116', col[j]] <=0:
            RA14[col[j+2]] = 'N/A'
        else:
            RA14[col[j + 2]] = cf.at['CF116', col[j+1]] / cf.at['CF116', col[j]]-1

        if f.at['F29',col[j+2]] == 0:
            RA15[col[j+2]] = 0
        else:
            RA15[col[j + 2]] = RA19[col[j+2]] *(1 - (- f.at['F64', col[j+2]]/f.at['F29', col[j+2]]))

    tempRA = { 'RA10':RA10, 'RA11':RA11,'RA12':RA12,'RA13':RA13,'RA14':RA14,'RA15':RA15,'RA19':RA19, 'RA20':RA20, 'RA21':RA21,\
             'RA24':RA24,'RA25':RA25,'RA26':RA26,'RA27':RA27,'RA30':RA30,'RA31':RA30,'RA32':RA32,'RA33':RA33,'RA34':RA34,'RA35':RA35,\
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

    for i in range(0,16):
        CR11[col[i]] = (f.at['F44', col[i]] + f.at['F49', col[i]]) / f.at['F57', col[i]]
        CR12[col[i+1]] = cf.at['CF17',col[i]]/((f.at['F44', col[i+1]] + f.at['F49', col[i+1]] + f.at['F44', col[i]] + f.at['F49', col[i]])/2)
        CR13[col[i+1]] = cf.at['CF24', col[i]] / ((f.at['F44', col[i + 1]] + f.at['F49', col[i + 1]] + f.at['F44', col[i]] + f.at['F49', col[i]]) / 2)
        CR17[col[i]] = f.at['F37', col[i]] / f.at['F48', col[i]]
        CR18[col[i]] = (f.at['F33', col[i]] + f.at['F34', col[i]]) / f.at['F48', col[i]]
        CR19[col[i]] = -f.at['F19', col[i]] / f.at['F20', col[i]]
        CR20[col[i]] = -f.at['F17', col[i]] / f.at['F20', col[i]]
        CR23[col[i]] = f.at['F25', col[i]] / f.at['F42', col[i]]
        CR25[col[i]] = f.at['F52', col[i]] / f.at['F42', col[i]]
        CR27[col[i]] = CR18[col[i]]
        CR29[col[i]] = CR19[col[i]]
        CR31[col[i]] = -365*f.at['F35', col[i]] / f.at['F13', col[i]]
        CR33[col[i+1]] = f.at['F12', col[i+1]] / f.at['F12', col[i]] -1

        if CR23[col[i]] <  -0.448:
            CR24[col[i]] = 0.083
        elif CR23[col[i]] < -0.161:
            CR24[col[i]] = -0.08
        elif CR23[col[i]] < -0.049:
            CR24[col[i]] = 0.072
        elif CR23[col[i]] < 0.001:
            CR24[col[i]] = 0.055
        elif CR23[col[i]] < 0.022:
            CR24[col[i]] = 0.045
        elif CR23[col[i]] < 0.038:
            CR24[col[i]] = 0.030
        elif CR23[col[i]] < 0.056:
            CR24[col[i]] = 0.025
        elif CR23[col[i]] < 0.079:
            CR24[col[i]] = 0.020
        else:
            CR24[col[i]] = 0.020

        if CR25[col[i]] < 0.175:
            CR26[col[i]] = 0.02
        elif CR25[col[i]] < 0.283:
            CR26[col[i]] = 0.022
        elif CR25[col[i]] < 0.385:
            CR26[col[i]] = 0.025
        elif CR25[col[i]] < 0.476:
            CR26[col[i]] = 0.030
        elif CR25[col[i]] < 0.558:
            CR26[col[i]] = 0.035
        elif CR25[col[i]] < 0.633:
            CR26[col[i]] = 0.045
        elif CR25[col[i]] < 0.709:
            CR26[col[i]] = 0.055
        elif CR25[col[i]] < 0.816:
            CR26[col[i]] = 0.070
        elif CR25[col[i]] < 1.030:
            CR26[col[i]] = 0.090
        else:
            CR26[col[i]] = 0.10


        if CR27[col[i]] < 0.382:
            CR28[col[i]] = 0.09
        elif CR27[col[i]] < 0.522:
            CR28[col[i]] = 0.065
        elif CR27[col[i]] < 0.809:
            CR28[col[i]] = 0.05
        elif CR27[col[i]] < 0.994:
            CR28[col[i]] = 0.042
        elif CR27[col[i]] < 1.2:
            CR28[col[i]] = 0.04
        elif CR27[col[i]] < 1.470:
            CR28[col[i]] = 0.035
        elif CR27[col[i]] < 1.890:
            CR28[col[i]] = 0.025
        elif CR27[col[i]] < 2.530:
            CR28[col[i]] = 0.020
        elif CR27[col[i]] < 4.550:
            CR28[col[i]] = 0.015
        else:
            CR28[col[i]] = 0.010


        if CR29[col[i]] < -17.4:
            CR30[col[i]] = 0.075
        elif CR29[col[i]] < -3.6:
            CR30[col[i]] = 0.09
        elif CR29[col[i]] < -0.139:
            CR30[col[i]] = 0.085
        elif CR29[col[i]] < 1.25:
            CR30[col[i]] = 0.070
        elif CR29[col[i]] < 2.21:
            CR30[col[i]] = 0.05
        elif CR29[col[i]] < 3.387:
            CR30[col[i]] = 0.030
        elif CR29[col[i]] < 5.21:
            CR30[col[i]] = 0.018
        elif CR29[col[i]] < 9.16:
            CR30[col[i]] = 0.018
        elif CR29[col[i]] < 23.8:
            CR30[col[i]] = 0.01
        else:
            CR30[col[i]] = 0.010


        if CR31[col[i]] < 0.365:
            CR32[col[i]] = 0.03
        elif CR31[col[i]] < 2.56:
            CR32[col[i]] = 0.035
        elif CR31[col[i]] < 14.24:
            CR32[col[i]] = 0.039
        elif CR31[col[i]] < 31.39:
            CR32[col[i]] = 0.040
        elif CR31[col[i]] < 50:
            CR32[col[i]] = 0.042
        elif CR31[col[i]] < 70.08:
            CR32[col[i]] = 0.045
        elif CR31[col[i]] < 91.98:
            CR32[col[i]] = 0.049
        elif CR31[col[i]] < 121.54:
            CR32[col[i]] = 0.055
        elif CR31[col[i]] < 174.47:
            CR32[col[i]] = 0.06
        else:
            CR32[col[i]] = 0.070

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

        CR35[col[i]] = (CR24[col[i]]+CR26[col[i]]+CR28[col[i]]+CR30[col[i]]+CR32[col[i]]+CR34[col[i]])/6

    tempCR = {
                'CR11':CR11, 'CR12':CR12,'CR13': CR13,'CR17': CR17,'CR18': CR18,'CR19': CR19,'CR20': CR20,'CR23': CR23, \
                'CR24': CR24,'CR25': CR25,'CR26': CR26,'CR27': CR27,'CR28': CR28,'CR29': CR29, 'CR30':CR30,'CR31': CR31,\
                'CR33':CR32,'CR33': CR33, 'CR34': CR34, 'CR35': CR35
                }

    dfCR = pd.DataFrame(tempCR)
    dfCRT = dfCR.transpose()
    return dfCRT


def save_xls(list_dfs, xls_path, codes):
    writer = pd.ExcelWriter(xls_path, engine='xlsxwriter')
    for n, m in enumerate(list_dfs):
        list_dfs[m].to_excel(writer, codes[m])
    writer.save()

if __name__ == "__main__":

    PPL = DBlist('C:/a/evalrawkissIP1905.db') # 리스트 호출~
    PPL.sort()
    conn = sqlite3.connect("c:/a/evalKisNonPubDATA1906_test.db")  # 새로운 db생성
    temp_df = {}

    for k, l in enumerate(PPL):
        raw = import_com(l,'C:/a/evalrawkissIP1905.db')     # 데이터 처리하여 DF로 리턴
        df1 = reindex(raw)

        df = build_FS(df1)  # df1 에 처리된 데이터 저장
        fa = ForecastingAss(df)  # Forecasting Assumption 작성

        fsF = build_FSFC(df, fa)  # raw를 받아 변환한 데이터와 fa데이터를 모아 FS Forecasting부분 작성
        fsAR = df.round(2)  # financial statement actual part
        fsFR = fsF.round(2)  # finanacial statement forecast part

        FS = pd.merge(left=fsAR, right=fsFR, how='outer', left_index=True, right_index=True)
        dup = dupont_analysis(FS)
        dupR = dup.round(2)
        wholeFS = pd.concat([FS,dupR], axis = 0)
        CF = Build_CF(wholeFS)

        CF = CF.round(3)
        RA = Build_RA(wholeFS, CF)

        RA = RA.round(3)
        CR = Build_CR(wholeFS, CF)

        CR = CR.round(4)
        wholeFS = wholeFS.drop(['G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R'], axis=1)
        CFa = CF.drop(['F','G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R'], axis=1)
        CF = CFa.rename(index=str, columns={"B": "C", "C": "D", "D": "E", "E": "F"}) #eval에는 한칸밀려서 있다. 그걸 한칸 밀어주는 작업
        RA = RA.drop(['G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R'], axis=1)
        CR = CR.drop(['G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R'], axis=1)
        Wholedata = pd.concat([wholeFS, CF, RA, CR], axis=0)
        WholedataN = Wholedata.rename(index=str, columns={"B": "2014", "C": "2015", "D": "2016", "E": "2017","F":"2018"})
        WholedataN.to_sql(l, conn, if_exists='replace', index_label='PAGECODE')
        #temp_df[k] = WholedataN
    #save_xls(temp_df, 'C:\\a\\kis_evalFS1906.xlsx', PPL)  # 새로운 엑셀파일 생성
    #del temp_df





