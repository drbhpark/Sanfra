#19년 5월 22일 현재 kisvalue 단일재무제표 EVAL용 Raw 데이터로 변환하는 프로그램.
#카타고리 정의를 대폭 수정함.
#

import requests
import pandas as pd
from pandas import Series, DataFrame
#import json
import sqlite3
from pandas import ExcelWriter
import time

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
    #df = pd.read_sql("SELECT * FROM '%s'" % com_code, con, index_col='ACCODE')
    df = pd.read_sql("SELECT * FROM '%s'" % com_code, con)
    return df


# 받아올 데이터 포맷 정리 루틴
def moddata(row_code):            # 라인별 임포트 모듈
    # temp=df.ix[row_code]
    temp = df.loc[row_code, '2014':'2018']
    temp_a = pd.to_numeric(temp, errors='coerce')
    temp_b = temp_a.fillna(0)
    temp_b = temp_b.div(1000)  # 원단위를 천원단위로 변환 (달러로 변환)
    return temp_b

def create_Eval_table_op(data):
# 항목매칭작업 - kisvalue에 단독제무제표로 올라온 모든 데이터를 받은 파일을 변환
    #손익계산서

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
    N_employee = moddata(49).mul(1000)


    temp = {'01 Sales': sales,
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

    df = pd.DataFrame(temp)      #dataframe으로 모으고
    df1 = df.transpose()        #축 변경

    #YR = ['2012/12', '2013/12', '2014/12', '2015/12', '2016/12', '2017/12(E)']
    #df2 = df1.rename(columns={'DATA1': YR[0], 'DATA2': YR[1], 'DATA3': YR[2], 'DATA4':
     #                           YR[3], 'DATA5': YR[4], 'DATA6': YR[5]})  # 인덱스 변경

    return df1

def ticker_finder(ticker):
    df = pd.read_excel('C:/a/koreantickers.xlsx',index_col = 'tick')
    a = df['name'][ticker]
    return a

def save_xls(list_dfs, xls_path, codes):
    writer = pd.ExcelWriter(xls_path, engine='xlsxwriter')
    for n, m in enumerate(list_dfs):
        list_dfs[m].to_excel(writer, codes[m])
    writer.save()

if __name__ == "__main__":

    #removeddb = []

    PPL = DBlist('C:/a/ext1905byKISticker.db') # 리스트 호출~ 갱신시 여기 바꿔주고
    temp_df = {}

    conn = sqlite3.connect("c:/a/evalrawkissIP1905.db")  # 새로운 db생성 - 갱신시 여기 바꿔주고

    for k, l in enumerate(PPL):
        try :
            df = import_com(l,'C:/a/ext1905byKISticker.db')    # 데이터 처리하여 DF로 리턴 - 갱신시 여기 바꿔주고
            #code = df['2018'][1]
            #sales2017 = df['2017'][3]
            #print(df,code,sales2017)
            #if code is None : #and sales2017 is not None:  #비상장주 처리시 쓰는 루틴
            #if sales2017 and code is not None: #상장주 처리시 쓰는 루틴
            df1 = create_Eval_table_op(df)   #df1 에 처리된 데이터 저장
            company_name = df.loc[2,'2018']
            CS = df1.ix['33 Number of Common Stocks']  # 보통주수는 따로 빼서 단위 보존, 천주단위
            EMP = df1.ix['34 Number of Employees']
            DEP = df1.ix['35 Depreciation']
            df1 = df1[:-3].div(1000)  # 천원단위를 백만원 단위로 변환(천불), 보통주수는 제외
            df1 = df1.round(2)
            CS = CS.div(1)
            DEP = DEP.div(1000)
            df1 = df1.append(CS)
            df1 = df1.append(EMP)
            df1 = df1.append(DEP)
            namemix = company_name + ' ' + l
            print(namemix,k)
            df1.to_sql(namemix, conn, if_exists='replace')
                #temp_df[k] = df1             #엑셀에 넣기위해 데이터 변환작업
            #else:
            #    pass
        except:
            pass
    #save_xls(temp_df, 'C:\\a\\EvalRawfromKISSIP_1808.xlsx', PPL)  # 새로운 엑셀파일 생성

