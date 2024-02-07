from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC

# from urllib.parse import quote
import chromedriver_autoinstaller
# from bs4 import BeautifulSoup
import re
import pandas as pd
import time

from datetime import datetime, timedelta
import traceback

import locale
# 로케일을 한국으로 설정
locale.setlocale(locale.LC_TIME, 'ko_KR.UTF-8')


# 파일 import
df = pd.read_csv('D:/작업폴더/웬즈데이 세일 정보.csv',encoding='utf-8')
df = df.dropna(subset=['brand_name'])
brand_code = pd.read_csv('D:/작업폴더/whensday_brand_code.csv')

# brand_code 추가
df['brand_code']=''

## brand_code_df를 딕셔너리로 변환
brand_code_dict = dict(zip(brand_code['brand_name'], brand_code['brand_value']))

## df에 brand_code 열 추가
df['brand_code'] = df['brand_name'].map(brand_code_dict)
nan_value = df[df['brand_name'].isnull()]['brand_name']

## 브랜드명이 일치하지 않을 경우 에러 창 
try:
    df['brand_code'] = df['brand_code'].astype('int')
except:
    for value in nan_value:
        print(f'{value}에서 이름이 일치하지 않습니다.')

# 상품명에 특수이모티콘 제거
#3 이모지 패턴 정의
emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # 이모티콘
        u"\U0001F300-\U0001F5FF"  # 심볼 및 픽토그램
        u"\U0001F680-\U0001F6FF"  # 교통 및 지도 심볼
        u"\U0001F1E0-\U0001F1FF"  # 국기 (iOS)
        "]+", flags=re.UNICODE)

## 'sale_name', 'sale_info'에서 이모지 제거
df['sale_name'] = df['sale_name'].apply(lambda x: emoji_pattern.sub(r'', x))
df['sale_info'] = df['sale_info'].apply(lambda x: emoji_pattern.sub(r'', x))

###########################################################################################

# 기초 변수 설정
urls= 'https://whensday.co.kr/login'
store_urls = 'https://whensday.co.kr/partner/'

id = 'mcorpor'
password = 'test1234'

path = 'D:/작업폴더/whensimg/'


# 웹드라이버 설정

## 브라우저 꺼짐 방지
chrome_options = Options()
chrome_options.add_experimental_option("detach", True)

## 드라이버 생성
chrome_options = webdriver.ChromeOptions()
# driver = webdriver.Chrome(service = Service('C:\Users\mco\Desktop\chromedriver-win64\chromedriver.exe'), options = chrome_options)
driver = webdriver.Chrome(service = Service(ChromeDriverManager().install()), options = chrome_options) 
# driver = webdriver.Chrome(service= Service(ChromeDriverManager(version="120.0.6099.234").install())) 
actions = ActionChains(driver)

## 브라우저 사이즈
driver.set_window_size(1900,1000)

## 웹페이지가 로드될 떄까지 2초 대기
driver.implicitly_wait(time_to_wait=2)
driver.get(url=urls)

# 로그인

id_ele = driver.find_element(By.XPATH,'//*[@id="email"]/input')
pass_ele = driver.find_element(By.XPATH,'//*[@id="password"]/input')

id_ele.send_keys(id)
pass_ele.send_keys(password)


login_xpath = '//*[@id="login-layout"]/main/form/button'

driver.find_element(By.XPATH,login_xpath).click()
time.sleep(1)

# 팝업창 제거
try : 
    driver.find_element(By.XPATH,'//*[@id="modal"]/section/div/section[2]/button[1]').click()
except :
    pass


old_store = ""

# 정보 입력
try: 
    for index, row in df.iterrows():
        # 데이터 입력
        store = row['brand_name']
        # store_value = row['brand_code']
        store_value = str(row['brand_code'])
        sale_name = row['sale_name']
        sale_info = row['sale_info']
        start_date = row['start_date']
        end_date = row['end_date']
        sale_url = row['url']
        sale_img_path = row['file_name']

        store_url = store_urls + store_value + '/sale'
        img_path = path + sale_img_path


        # 데이터 입력

        # 스토어 페이지로 이동
        if store != old_store : 
            driver.get(url=store_url)
            driver.implicitly_wait(time_to_wait=2)
        else:
            pass
                # 브랜드가 같을 경우, 페이지 이동 없이 해당 페이지에서 그대로 진행
        

        # 세일정보 입력
        name_ele = driver.find_element(By.XPATH,'//*[@id="event-register"]/section/section/section[1]/div/div[1]/input')
        info_ele = driver.find_element(By.XPATH,'//*[@id="event-register"]/section/section/section[1]/div/div[2]/input')
                ## 해당 부분, 추후 웬즈데이 업데이트 시 수정예정
        name_ele.send_keys(sale_name)
        info_ele.send_keys(sale_info)


        # URL 값 입력
        url_ele = driver.find_element(By.XPATH,'//*[@id="event-register"]/section/section/section[4]/div/div/input')
        url_ele.send_keys(sale_url)

        # 파일 업데이트
        driver.find_element(By.XPATH, '//*[@id="event-register"]/section/section/section[3]/input').send_keys(img_path)

        # 세일 기간 입력
        ## 날짜값 전처리
        start_date2 = datetime.strptime(start_date, "%Y-%m-%d")
        start_month = start_date2.month

        end_date2 = datetime.strptime(end_date, "%Y-%m-%d")
        end_month = end_date2.month

        current_date = datetime.now()

        months_difference = (end_date2.year - start_date2.year) * 12 + (end_month - start_month)

        formatted_startdate = start_date2.strftime("%Y년 %B %#d일 %A")
        # formatted_startdate = start_date2.strftime("%Y")+'년 ' + start_date2.strftime("%m")+"월 " + start_date2.strftime("%#d")+"일 "+ start_date2.strftime("%A")
        formatted_enddate = end_date2.strftime("%Y년 %B %#d일 %A")
        # formatted_enddate = end_date2.strftime("%Y")+'년 ' + end_date2.strftime("%m")+"월 " + end_date2.strftime("%#d")+"일 "+ end_date2.strftime("%A")

        ## 기간 입력

        ## 시작연도 = 현재연도일 경우
        if start_date2.year == current_date.year:
            driver.find_element(By.XPATH,'//*[@id="event-register"]/section/section/section[2]/div/div/section').click()
            time.sleep(3)

            ## <시작기간 입력>
            month_diff=abs(start_month - current_date.month)
            # 가능한 경우의 수 세팅
            if start_month > current_date.month: # 시작달이 현재달보다 미래
                for i in range(0, month_diff) : 
                    driver.find_element(By.XPATH,'//*[@id="event-register"]/section/section/section[2]/div/div/div/div/div[1]/div/div[1]/div/button[2]').click()
  
            elif start_month < current_date.month: # 시작달이 현재달보다 과거
                for i in range(0, month_diff) : 
                    driver.find_element(By.XPATH,'//*[@id="event-register"]/section/section/section[2]/div/div/div/div/div[1]/div/div[1]/div/button[1]').click()

            else : # 시작달과 현재달이 동일
                pass

            time.sleep(3)
            driver.find_element(By.XPATH, f'//*[@aria-label="{formatted_startdate}"]').click()

            ## <종료기간 입력>
            if end_month > start_month : # 종료달이 시작달보다 미래
                for i in range(0,months_difference) :    
                    driver.find_element(By.XPATH,'//*[@id="event-register"]/section/section/section[2]/div/div/div/div/div[1]/div/div[1]/div/button[2]').click()
 
            else: # 종료달이 시작달보다 과거이거나 동일(과거인 경우의 수는 없음)
                pass

            time.sleep(3)
            driver.find_element(By.XPATH, f'//*[@aria-label="{formatted_enddate}"]').click()

            driver.find_element(By.XPATH,'//*[@id="event-register"]/section/section/section[2]/div/div/section').click()
        
        ## 시작연도 != 현재연도일 경우
        else :
            driver.find_element(By.XPATH,'//*[@id="event-register"]/section/section/section[2]/div/div/section').click()
            time.sleep(2)  

            # <시작기간 입력>
            ## 달 설정
            month_diff=abs(current_date.month + 12 - start_month)
            for i in range(0, month_diff) : # 시작달은 무조건 현재달보다 과거 시점
                    driver.find_element(By.XPATH,'//*[@id="event-register"]/section/section/section[2]/div/div/div/div/div[1]/div/div[1]/div/button[1]').click()
            time.sleep(0.5)

            ## 구체적 일자 클릭
            driver.find_element(By.XPATH, f'//*[@aria-label="{formatted_startdate}"]').click()
            time.sleep(0.5)

            # <종료기간 입력>
            for i in range(0,months_difference) :  # 종료달은 시작달보다 무조건 미래
                driver.find_element(By.XPATH,'//*[@id="event-register"]/section/section/section[2]/div/div/div/div/div[1]/div/div[1]/div/button[2]').click()
            
            ## 구체적 일자 클릭
            time.sleep(0.5)
            driver.find_element(By.XPATH, f'//*[@aria-label="{formatted_enddate}"]').click()
            time.sleep(0.5)

        time.sleep(1.5)

        driver.find_element(By.XPATH,'//*[@id="event-register"]/section/section/section[2]/div/div/section').click()

        # 최종 저장
        driver.find_element(By.XPATH, '//*[@id="event-register"]/header/div/button[2]').click()

        old_store = store
            # 다음 등록 정보와 비교 위해, 현재 brand_name 값 남겨놓음
        print(f"{store}저장완료")

        # 업데이트 완료한 열은 삭제 후 파일 업데이트 (중복방지목적)
        df.drop(index, inplace=True)
        df.to_csv('D:/작업폴더/웬즈데이 세일 정보.csv',encoding='utf-8', index=False)

        # 홈으로 돌아가기 => 바로 url 링크 이동하는 방식으로 바꿨기 때문에 필요 없음
        # driver.find_element(By.XPATH, '//*[@id="page-layout"]/div[1]/div/h1/a').click()
        time.sleep(3)
        
    # 모든 세일정보를 정상적으로 업데이트 완료
    print('업데이트 완료')

except Exception as e:
    print(f'{sale_name}값 등록 오류')
    print(traceback.format_exc())
    
finally : 
    driver.quit()
