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
df99 = pd.read_excel('C:/a/ext/1999s.xlsx')

length = len(df18)
col = list(df18.columns)
con = sqlite3.connect('C:/a/ext1906byKISticker18.db')

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
df99.index = df99['KIS']

# 받아올 데이터 포맷 정리 루틴
def moddata(column_code):            # 라인별 임포트 모듈
    # temp=df.ix[row_code]
    temp = df[col[column_code]]
    temp_a = pd.to_numeric(temp, errors='coerce')
    temp_b = temp_a.fillna(0)
    temp_b = temp_b.div(1000000)  # 원단위를 백만원단위로 변환
    return temp_b

def evalraw_transformer(df):
# 항목매칭작업 - kisvalue에 단독제무제표로 올라온 모든 데이터를 받은 파일을 변환
    #손익계산서
    #col = df.columns
    name = df['Name']
    stockcode = df['Stock']

    sales = moddata(3)  # 매출액
    COGS = moddata(4)  # 매출원가
    COGS = COGS.mul(-1) #비용은 음수
    SGATotal = moddata(5)  # 한국재무제표상 전체 판매비와관리비
    Dep = moddata(7)  # 감가상각비
    Amo = moddata(6)  # 무형자산상각비
    DeAm = Dep + Amo  # 감가상각비+무형자산상각비
    DeAm = DeAm.mul(-1)
    RD1 = moddata(8) #연구비
    RD2 = moddata(9)  #경상연구개발비
    RD3 = moddata(10)  #경상개발비
    RDExpense = RD1 + RD2 + RD3 # 연구비 총계
    RDExpense = RDExpense.mul(-1) # 비용은 음수
    SGA = SGATotal + RDExpense + DeAm # 데이터 테이블상 SGA는 연구개발비, 감가상각과 분리
    SGA = SGA.mul(-1) # 비용은 음수
    Interest_Exp = moddata(13)  # 금융비용
    Interest_Exp = Interest_Exp.mul(-1)
    #Interest_Income = moddata(14)+moddata(15)
    #Net_Int = Interest_Income + Interest_Exp #이자수익 - 이자비용
    #Min_int_E = moddata(17) # 소수주주지분순이익
    #Min_int_L = moddata(16) #소수주주지분순손실
    #Min_int_L = Min_int_L.mul(-1)
    Min_Int_Earning =  0
    #Nonopincome = moddata(11)  # 영업외수익
    #Nonoploss = moddata(12) - Interest_Exp # 영업외비용에서 이자비용을 다시 빼줌
    #Nonoploss = Nonoploss.mul(-1) # 비용은 음수
    Temp_EBT = moddata(17) #영업이익(손실)/125000
    Temp_EBIT = moddata(19) # 법인세비용차감전 계속사업이익
    Diff_I = Temp_EBIT - Temp_EBT
    Nonopincomes = Diff_I +Interest_Exp
    #Nonopincomes = Nonopincome + Nonoploss
    IncomeTax = moddata(18)  # 법인세비용
    IncomeTax = IncomeTax.mul(-1) # 비용은 음수
    Otherincome = 0 # 지분법회사 이익-손실
    ExtItems = moddata(16) #  중단사업이익
    #재무제표
    CurrAcc = moddata (20) # 당좌자산
    OpCash = moddata(21)  # 현금및현금성자산
    Market_Sec1 = moddata(22)  # 단기금융상품
    Market_Sec2 = moddata(23)  #단기투자증권
    Opc_Msec = OpCash + Market_Sec1 + Market_Sec2 # 현금및현금성자산
    Receivables = moddata(24)  # 매출채권
    Inventories = moddata(25)  # 재고자산
    OCA = CurrAcc - Opc_Msec - Receivables   # 기타유동자산 = 당좌자산 - 현금성자산-매출채권
    PPE = moddata(28)  # 유형자산
    Investment = moddata(27)  # 투자자산
    Intangible = moddata(29)  # 무형자산
    OA1 = moddata(30)  # 기타비유동자산
    OA2 = moddata(31)  # 이연자산
    #OA3 = moddata(32)  # 기타금융업자산
    #OA4 = moddata(33)  #연결조정차
    Otherassets = OA1+OA2
    #채무
    Acc_payable = moddata(43)  # 매입채무
    IncomeTax_payable = moddata(33)  # 이연법인세부채(유동부채아래)
    CurrLiab = moddata(32) # 유동부채
    OthCurrLiab = moddata(44) #기타유동부채
    CurrentDebt =  CurrLiab - Acc_payable - IncomeTax_payable - OthCurrLiab  # 유동부채
    noncurrentliab = moddata(34)  # 비유동부채(계)
    DefTax = moddata(35)  # 이연법인세부채(비유동부채아래)
    OthLiab = moddata(45) + moddata(36)  # 기타비유동부채+이연부채
    longtermdebt = noncurrentliab - DefTax - OthLiab
    Minority_interest = 0  # 외부주주지분
    Pref_stock = moddata(38) # 우선주자본금
    Paidincommcap = moddata(37)  # 보통주자본금
    N_CommStock = moddata(47) #보통주발행주식수
    PreferredDiv  =  moddata(50) * (Pref_stock/(Pref_stock + Paidincommcap) )           #우선주배당 - 자본잉여금 유출 기타.
    PreferredDiv = PreferredDiv.mul(-1)
    retainedequity = moddata(39) #자본잉여금
    othercapital = moddata(41) #자본조정
    otherretained = moddata(42) #기타포괄손익누계액
    retainedprofit = moddata(40) #이익잉여금
    Retained_earnings = retainedequity+othercapital+otherretained+retainedprofit
    Common_div = moddata(50) + PreferredDiv  # 배당금의지급
    Common_div = Common_div.mul(-1)
    N_employee = moddata(49).mul(1000000)


    temp = {
         '0 Stockcode' : stockcode,
         '00 Company Name' : name,
         '01 Sales': sales,
        '02 COGS': COGS,
        '03 RDExpense': RDExpense,
        '04 SGA': SGA,
        '05 Depreciation and Amortization': DeAm,
        '06 Interest_Expenses': Interest_Exp,
        '07 Non operational income': Nonopincomes,  # 영업외수익
        '08 Income Tax': IncomeTax,  # 법인세비용
        '09 Minority Interest in Earning' :Min_Int_Earning,            # 지분법상 이익
        '10 Other Income(Loss)' :  Otherincome,                             # 기타수익
        '11 Extra Items or discontinued business income': ExtItems,  # 특별기타항목-통상적이지 않은 이익이나 매각 등 - 중단사업이익
        '12 Preferred Dividend': PreferredDiv, #자본잉여금 감소의 기타부분
        '13 Operating Cash and Marketable Securiteis': Opc_Msec,  # 현금및현금성자산
        '14 Receivables': Receivables,  # 매출채권
        '15 Inventories': Inventories,  # 재고자산
        '16 Other Current Assets': OCA,  # 기타유동자산
        '17 PP&E': PPE,  # 설비자산
        '18 Investments': Investment,  # 투자자산
        '19 Intangibles': Intangible,  # 무형자산
        '20 Other Assets': Otherassets,  # 기타비유동자산
        '21 Current Debt': CurrentDebt,  # 유동부채
        '22 Account Payable': Acc_payable,  # 매입채무
        '23 Income Tax Payable': IncomeTax_payable,  # 이연법인세부채
        '24 Other Current Liabilities': OthCurrLiab,  # 기타유동부채
        '25 Long term debt': longtermdebt,
        '26 Other Liabilities': OthLiab,
        '27 Deferred Tax': DefTax,  # 이연법인세부채
        '28 Minority Interests': Minority_interest,  # 외부주주지분
        '29 Preferred Stock': Pref_stock,  # 우선주자본금 eval p188
        '30 Paid in Common Capital': Paidincommcap,  # 보통주자본금
        '31 Retained Earnings': Retained_earnings,  # 이익잉여금
        '32 Common Dividends': Common_div,  # 배당금의지급
        '33 Number of Common Stocks': N_CommStock,  # 보통주수
        '34 Number of Employees': N_employee,
        '35 Depreciation': Dep
          }

    df1 = pd.DataFrame(temp)      #dataframe으로 모으고
    return df1


conn = sqlite3.connect("C:/a/ext19yrs.db")

year = ['1999','2000', '2001','2002','2003','2004', '2005', '2006','2007','2008', '2009', '2010', '2011','2012','2013','2014', '2015', '2016', '2017','2018']

df = df99
df99a = evalraw_transformer(df)
df = df00
df00a = evalraw_transformer(df)
df = df01
df01a = evalraw_transformer(df)
df = df02
df02a = evalraw_transformer(df)
df = df03
df03a = evalraw_transformer(df)
df = df04
df04a = evalraw_transformer(df)
df = df05
df05a = evalraw_transformer(df)
df = df06
df06a = evalraw_transformer(df)
df = df07
df07a = evalraw_transformer(df)
df = df08
df08a = evalraw_transformer(df)
df = df09
df09a = evalraw_transformer(df)
df = df10
df10a = evalraw_transformer(df)
df = df11
df11a = evalraw_transformer(df)

df = df12
df12a = evalraw_transformer(df)
df = df13
df13a = evalraw_transformer(df)
df = df14
df14a = evalraw_transformer(df)
df = df15
df15a = evalraw_transformer(df)
df = df16
df16a = evalraw_transformer(df)
df = df17
df17a = evalraw_transformer(df)
df = df18
df18a = evalraw_transformer(df)


df99a.to_sql('1999', conn, if_exists='replace')
df00a.to_sql('2000',conn,if_exists='replace' )
df01a.to_sql('2001',conn,if_exists='replace' )
df02a.to_sql('2002',conn,if_exists='replace' )
df03a.to_sql('2003',conn,if_exists='replace' )
df04a.to_sql('2004',conn,if_exists='replace' )
df05a.to_sql('2005',conn,if_exists='replace' )
df06a.to_sql('2006',conn,if_exists='replace' )
df07a.to_sql('2007',conn,if_exists='replace' )
df08a.to_sql('2008',conn,if_exists='replace' )
df09a.to_sql('2009',conn,if_exists='replace' )
df10a.to_sql('2010',conn,if_exists='replace' )
df11a.to_sql('2011',conn,if_exists='replace' )
df12a.to_sql('2012',conn,if_exists='replace' )
df13a.to_sql('2013',conn,if_exists='replace' )
df14a.to_sql('2014',conn,if_exists='replace' )
df15a.to_sql('2015',conn,if_exists='replace' )
df16a.to_sql('2016',conn,if_exists='replace' )
df17a.to_sql('2017',conn,if_exists='replace' )
df18a.to_sql('2018',conn,if_exists='replace' )







