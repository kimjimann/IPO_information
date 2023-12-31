import requests 
from bs4 import BeautifulSoup
import csv
import pandas as pd
from datetime import datetime
import array as arr
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

#공모주청약일정 정보
def get_IPO_subscription_data_from_38(page=1): 
    total_data = [] #모든 데이터 (상장일, 환불일 제외)
    listing_refunding_data = [] #상장일 환불일 데이터

    #공모주 청약일정 데이터 수집
    for p in range(1, page + 2): # 페이지당 노출 행이 30개
       fullUrl = 'http://www.38.co.kr/html/fund/index.htm?o=k&page=%s' %p
       response = requests.get(fullUrl, headers={'User-Agent': 'Mozilla/5.0'})
       html = response.text
       soup = BeautifulSoup(html, 'lxml')
       data = soup.find('table', {'summary': '공모주 청약일정'})
       data = data.find_all('tr')[2:]
       total_data = total_data + data

    stock_name_list = [] #종목명
    ipo_schedule_list = [] #공모주일정
    confirmed_ipo_price_list = [] #확정공모가
    desired_ipo_price_list = [] #희망공모가
    subscription_competition_rate_list =[] #청약경쟁률
    underwriting_firm_list = [] #주간사
    name_analyze_list = [] #상세분석URL
    listing_data_list = [] #상장일
    RefundDate_list= [] #환불일
    

    #열(row)별 파싱
    for row in range(0, len(total_data)):
         data_list = total_data[row].text.replace('\xa0', '').replace('\t\t', '').split('\n')[1:-1]
         if len(data_list) < 6:
            continue
         stock_name_list.append(data_list[0].strip())
         ipo_schedule_list.append(data_list[1].strip())
         confirmed_ipo_price_list.append(data_list[2].strip())
         desired_ipo_price_list.append(data_list[3].strip())
         subscription_competition_rate_list.append(data_list[4].strip())
         underwriting_firm_list.append(data_list[5].strip())

   #상세분석 URL a 태그 파싱
    for row in range(0, len(total_data)):
    # 각 row에서 'a' 태그를 찾습니다.
     a_tag = total_data[row].find('a')
    # 'href' 속성을 추출하고 리스트에 추가합니다.
     href = a_tag['href']
     name_analyze_list.append(href)

   #상장,환불일 구하기 (상세분석 URL을 토대로 4번째 5번째 테이블 값인 환불일, 상장일을 가져옴)
    for p in name_analyze_list: # 60까지
       fullUrl = 'http://www.38.co.kr%s' %p 
       response = requests.get(fullUrl, headers={'User-Agent': 'Mozilla/5.0'})
       html = response.text
       soup = BeautifulSoup(html, 'lxml')
       data = soup.find('table', {'summary': '공모청약일정'})
       RefundDate_list.append(data.find_all('tr')[4].find_all('td')[1].get_text(strip=True)) #환불일
       listing_data_list.append(data.find_all('tr')[5].find_all('td')[1].get_text(strip=True)) #상장일
       
    #청약 일정 변수
    schedule = pd.DataFrame({'종목명': stock_name_list,
                             '공모주일정': ipo_schedule_list,
                             '환불일': RefundDate_list,
                             '상장일': listing_data_list,
                             '확정공모가': confirmed_ipo_price_list,
                             '희망공모가': desired_ipo_price_list,
                             '청약경쟁률': subscription_competition_rate_list,
                             '주간사': underwriting_firm_list
                             })
    return schedule

IPO_subscription = get_IPO_subscription_data_from_38()

#수요예측일정 정보
def get_IPO_demand_prediction_data_from_38(page=1): 
   total_data = []
   for p in range(1, page + 3): #페이지당 노출 행이 20개
      fullUrl = 'http://www.38.co.kr/html/fund/index.htm?o=r&page=%s' %p
      response = requests.get(fullUrl, headers={'User-Agent': 'Mozilla/5.0'})
      html = response.text
      soup = BeautifulSoup(html, 'lxml')
      data = soup.find('table', {'summary': '수요예측일정'})
      data = data.find_all('tr')[2:]
      total_data = total_data + data
      
   # 종목명 추출 주석 stock_name_list = [] #종목명 row:0
   demand_prediction_date_list = [] # 수요예측일 row:1
   ipo_amount_list = []  # 공모금액(백만) tr:4

   for row in range(0, len(total_data)):
      data_list = total_data[row].text.replace('\xa0', '').replace('\t\t', '').split('\n')[1:-1]
      if len(data_list) < 6:
         continue
      # 종목명 추출 주석 stock_name_list.append(data_list[0].strip())
      demand_prediction_date_list.append(data_list[1].strip())
      ipo_amount_list.append(data_list[4].strip())
       
   #수요예측정보
   prediction = pd.DataFrame({#종목명 추출 주석 '종목명': stock_name_list,
                              '수요예측일': demand_prediction_date_list,
                              '공모금액(백만)': ipo_amount_list})
   return prediction
IPO_prediction = get_IPO_demand_prediction_data_from_38()

#수요예측결과 정보
def get_IPO_demand_forecast_data_from_38(page=1): 
   total_data = []
   for p in range(1, page + 3): #페이지당 노출 행이 20개
      fullUrl = 'http://www.38.co.kr/html/fund/index.htm?o=r1&page=%s' %p
      response = requests.get(fullUrl, headers={'User-Agent': 'Mozilla/5.0'})
      html = response.text
      soup = BeautifulSoup(html, 'lxml')
      data = soup.find('table', {'summary': '수요예측결과'})
      data = data.find_all('tr')[2:]
      total_data = total_data + data
      
   #stock_name_list = [] # 종목명 row:0
   #demand_prediction_date_list = [] # 수요예측일 row:1
   #desired_ipo_price_list = [] # 희망공모가 row:2
   #confirmed_ipo_price_list = [] # 확정공모가 row:3
   #ipo_amount_list = []  # 공모금액(백만) tr:4
   institutional_competition_rate_list = [] #기관경쟁률 tr:5
   obligatory_holding_and_offering = [] #의무보유확약 tr:6


   for row in range(0, len(total_data)):
      data_list = total_data[row].text.replace('\xa0', '').replace('\t\t', '').split('\n')[1:-1]
      if len(data_list) < 6:
         continue
      #stock_name_list.append(data_list[0].strip())
      #demand_prediction_date_list.append(data_list[1].strip())
      #desired_ipo_price_list.append(data_list[2].strip())
      #confirmed_ipo_price_list.append(data_list[3].strip())
      #ipo_amount_list.append(data_list[4].strip())
      institutional_competition_rate_list.append(data_list[5].strip())
      obligatory_holding_and_offering.append(data_list[6].strip())
       

   #수요예측정보
   result = pd.DataFrame({ #'종목명': stock_name_list,
                           #  '수요예측일': demand_prediction_date_list,
                           #   '희망공모가': desired_ipo_price_list,
                           #   '확정공모가': confirmed_ipo_price_list,
                           #   '공모금액(백만)': ipo_amount_list,
                              '기관경쟁률' : institutional_competition_rate_list,
                              '의무보유확약' : obligatory_holding_and_offering})
   return result
IPO_forecast = get_IPO_demand_forecast_data_from_38()

#청약일정, 수요예측 합치기
IPO_merge_sub_pre = pd.concat([IPO_prediction,IPO_subscription], axis=1) #axsi=1 열(column)별로 합치기

#확정공모가가 "-" 이면 기관경쟁률, 의무보유확약에 공백이 있는 dataframe을 생성 (그후 예측결과 dataframe을 뒤에 붙이기)
def get_IPO_add_space(): #수요예측결과 공백 채우기

   institutional_competition_rate_list = [] #기관경쟁률
   obligatory_holding_and_offering = [] #의무보유확약

   for row in range(0,60):
      if IPO_merge_sub_pre.loc[row,"확정공모가"] == "-" :
         institutional_competition_rate_list.append(" ")
         obligatory_holding_and_offering.append(" ")

   add_space = pd.DataFrame({ '기관경쟁률' : institutional_competition_rate_list,
                           '의무보유확약' : obligatory_holding_and_offering})
   
   return add_space
IPO_space = get_IPO_add_space()

#공백 dataframe에 수요예측결과 붙이기
IPO_merge_forecast = pd.concat([IPO_space,IPO_forecast], axis=0, ignore_index=True) #axis=0 행(row)별로 합치기 / ignore_index=True 인덱스 합치기

#청약일정, 수요예측에 공백들어간 수요예측결과 붙이기
IPO_merge = pd.concat([IPO_merge_sub_pre, IPO_merge_forecast], axis=1)

#수수료.xlsx 파일에서 증권사명, 수수료 불러오기
fee_excel = pd.read_excel('D:\React\python_스크래핑\공모주청약수수료_증권사별.xlsx')
fee_dict = dict(zip(fee_excel.iloc[:, 1], fee_excel.iloc[:, 3]))

#print(len(column_data)) #결과: 76
#print((column_data[0])) #주간사 명: IBK투자증권
#print(column_data[1]) #주간사 명: 미래에셋증권,삼성증권
#print(len(fee_dict)) #주간사 별 수수료 길이: 23
#print(fee_dict[column_data[1]])
#print(fee_dict[column_data_list[1]]) #key 값 별 value 출력 방법
#column_data_list[data_2]

fee_total = []  # 주간사별 수수료 변수
IPO_merge['주간사'].fillna('', inplace=True)
column_data = IPO_merge['주간사'].values.tolist()  # 주간사 열 데이터 list로 변환

for agencies in column_data:
    agencies_list = [agency.strip() for agency in agencies.split(',')]  # 쉼표로 구분된 주간사를 리스트로 분리
    fees = [f"{agency}:{fee_dict.get(agency, '수수료 정보 없음')}" for agency in agencies_list]  # 각 주간사에 대한 수수료 매핑
    fee_total.append(fees)

IPO_merge['주간사별 수수료'] = fee_total #주간사별 수수료를 IPO_merge 행에 추가
      
#파일명에 금일날짜 추가
#금일 날짜 받아오기
today = datetime.today().strftime('%Y%m%d_%H%M%S')

# 파일 경로 삽입하여 원하는 곳에 저장
# f-string 사용해 문자열에 today 변수 삽입
# 통합 정보 출력
IPO_merge.to_excel(f'D:\React\python_스크래핑\공모주청약정보_엑셀\{today} - 통합 공모주 정보.xlsx')

#스프레드시트에 엑셀 복사
# 구글 API 인증 정보 로드
json_file_path = "D:\\React\\python_스크래핑\\exemplary-vista-402211-d49b6d696870.json"
gc = gspread.service_account(json_file_path)
# 스프레드 시트 주소 연결
spreadsheet_url = "https://docs.google.com/spreadsheets/d/11M6ineiwWlFOzfkvd3tsDUaQ27gogZISdFQ_Tnfha9w/edit"
doc = gc.open_by_url(spreadsheet_url)

#워크시트 "이름"으로 열기ㅣ
worksheet = doc.worksheet("공모주데이터")
#워크시트를 데이터 프레임으로 업데이트
set_with_dataframe(worksheet, IPO_merge)