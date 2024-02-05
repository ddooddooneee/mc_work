import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime,timedelta
import re
import os
import urllib.request
import json
import time
from dateutil.parser import parse


def daily_api_run(start_date, end_date, dt_yester = '2023-12-05') :
    '''
    네이버 API 조회하는 코드
    1. 상품명 간단하게 정제 (API 결과 조회를 해치는 키워드 제거)
    2. 기본에 API 결과로 보유중인 상품명은 조회 x
    3. 상품명:ITEMID 구조의 dic를 만들고, 상품명 API 조회가 끝나면 상품명,ITEMID,브랜드/제조사, 카테고리 매칭해서 파일로 도출
    '''
    start_date = start_date
    end_date = end_date
    if start_date != end_date : # 날짜 다른 경우와 같은 경우의 파일 생성
        report_file_name = f'./리포트_도출결과/{start_date}_{end_date}_냉세에_리포트.csv'
    else :
        report_file_name = f'./리포트_도출결과/{start_date}_냉세에_리포트.csv'
    
    # 파일 불러옴(리포트, 기존에 적재시킨 API_결과 파일)
    df = pd.read_csv(report_file_name,low_memory = False)
    # dt_yester = (dt_now - timedelta(days = 1)).strftime('%Y_%m_%d')
    api_file_name = f'./api_결과/API_취합.csv'
    api_df = pd.read_csv(api_file_name,low_memory=False)
    not_null_api_df = api_df[~api_df['cat1'].isnull()].reset_index(drop = True)

    def remove_stopwords(df1) :
        '''
        상품명 정제 코드
        - 출고, 가격 정보, ~~무료, ~~설치, ~~배관, 홈쇼핑, ~~증정 등등 
        '''
        df1['PRDT_NM'].fillna('', inplace = True)
        df1['AFTER_PRDT_NM'] = df1['PRDT_NM'].apply(lambda x : re.sub('[0-9]{1,2}일내 출고|[0-9]{1,2}일내출고','',x).strip())
        df1['AFTER_PRDT_NM'] = df1['AFTER_PRDT_NM'].apply(lambda x: x.replace('[','').replace(']','').strip())
        df1['AFTER_PRDT_NM'] = df1['AFTER_PRDT_NM'].apply(lambda x : re.sub('[-+]$','',x).strip())
        df1['AFTER_PRDT_NM'] = df1['AFTER_PRDT_NM'].apply(lambda x : re.sub('[0-9.,]{1,10}만원대|[0-9,.]{1,10}만원|[0-9.,]{1,10}만|쿠폰가|최종가|[가-힣A-Z]{1,2}단독|월?[0-9,.]{1,10}원|[0-9가-힣A-Z]{1,10}별도','',x).strip())
        df1['AFTER_PRDT_NM'] = df1['AFTER_PRDT_NM'].apply(lambda x : re.sub('^최대혜택가|^최대 혜택가|^카드혜택|^혜택가:|혜택가|&quot;|할인 중복쿠폰|특별할인판매중|롯데백화점세일페스타|히트상품','',x).strip())
        df1['AFTER_PRDT_NM'] = df1['AFTER_PRDT_NM'].apply(lambda x : re.sub('전국기본설치비무료|기본설치비무료|기본설치비 별도|설치비 별도|설치비별도|기본설치[/s]?포함|전국.기본설치무|배송비표 참조|별배송비상이|별 배송비 상이|배송비상이|배송비 상이|배송비 별도|배송비|설치비 별도|설치비|ㅣOKㅣ|로지텍기사','',x).strip())
        df1['AFTER_PRDT_NM'] = df1['AFTER_PRDT_NM'].apply(lambda x : re.sub('[0-9가-힣]{0,6}(?<!냉장고)배송|폐가전수거|폐가전 수거|사다리차 배달비|배달비|[가-힣]{1,3}른$|ㅣ써니77ㅣ|[0-9가-힣]{1,4}도착|[TV]{1,2}상품|상품 [0-9.]{1,3}\s|[0-9가-힣]{1,6}설치','',x).strip())
        df1['AFTER_PRDT_NM'] = df1['AFTER_PRDT_NM'].apply(lambda x : re.sub('사은품증정|[_]?사업자전용|[가-힣A-Z]{1,3}쇼핑|NS홈|행사가','',x).strip())
        df1['AFTER_PRDT_NM'] = df1['AFTER_PRDT_NM'].apply(lambda x : re.sub(r'[(]출시기념 할인 / 오후3시 당일출고[)]|[(]서울인근 [)]|[(]추천[)]|[(]최대  [)]|[(] [)]|[(]소상공인 [0-9]{1,2}% 지원[])]','',x).strip())
        df1['AFTER_PRDT_NM'] = df1['AFTER_PRDT_NM'].apply(lambda x : re.sub('^[A-Z]{1}\s|\s[A-Z가-힣/]{1}$|^[-+비쓱]{1}\s|^[.]\s|^[/]{1}|[/]{1}$|^갤러리아[_]?|희망일|직방|[^가-힣0-9A-Z][_A-Z0-9가-힣]{1,3}마켓','',x).strip())
        df1['AFTER_PRDT_NM'] = df1['AFTER_PRDT_NM'].apply(lambda x : re.sub(r'[+]{1,2}$|^[()_]{1,2}|최대할인|최대','',x).strip())
        df1['AFTER_PRDT_NM'] = df1['AFTER_PRDT_NM'].apply(lambda x : re.sub('[0-9가-힣A-Z]{1,10}별도|[가-힣0-9A-Z]{1,10}대상|부산|울산|창원|대구|양산|김해|경기|용인|수원|성남|광주|이천|안성|평택|오산|화성|안산|시흥|부천|광명|군포|의왕|안양|과천|여주|원주','',x).strip())
        df1['AFTER_PRDT_NM'] = df1['AFTER_PRDT_NM'].apply(lambda x : re.sub('^[^가-힣0-9A-Z]{1,3}|^공식_|^[가-힣A-Z0-9]{1}[\s]|[~전국A-Z0-9]{1,10}가능|단하루','',x).strip())
        df1['AFTER_PRDT_NM'] = df1['AFTER_PRDT_NM'].apply(lambda x : re.sub('빅스마지막|로그인 결제가 ◀|알루$|동배$|동배관$|대량구매|알루미늄배관전국무료','',x).strip())
        df1['AFTER_PRDT_NM'] = df1['AFTER_PRDT_NM'].apply(lambda x : re.sub(r'[()]{1,2}$','',x).strip())

        return df1

    df = remove_stopwords(df)

    # 고유 상품명 추출
    prdt = df['AFTER_PRDT_NM'].unique()
    prdt = prdt.tolist()

    # 순회하며 기존 저장 값과 비교하여 기존 값에 존재하면 제외
    for nm in tqdm(prdt) :
        if nm in not_null_api_df['PRDT_NM(ORIGIN)'].tolist() :
            prdt.remove(nm)


    # prdt_nm:itemid 형식으로 dic 저장 
      ## => 반복되서 수집되는 상품에 대해서 불필요한 데이터 수집을 줄이기 위해 필요한 과정
    prdt_dic = {}
    for prod in tqdm(prdt) :
        id = df[df['AFTER_PRDT_NM'] == prod]['itemId'].values[0]
        prdt_dic[prod] = id

    for key,val in prdt_dic.items():
        prdt_dic[key] = int(val)


    def shopping_api(keyword,id_idx = 0,secret_idx = 0) :
        '''
        상품명 순회하며 shopping api 조회하는 코드
        '''
        cat_keyword = keyword
        encText = urllib.parse.quote(cat_keyword)
                    ## 키워드 값을 URL-safe한 형식으로 인코딩
                    ## 검색키워드 값
        disText = urllib.parse.quote("40")
                  ## 상위 '40'개 검색값
        # filter = urllib.parse.quote("naverpay")
        # exc = urllib.parse.quote("cbshop:rental:used")

        # 네이버 API id와 Secret_id - 5개 기입 (최대 10개 가능)
        id_list = []
        secret_list = []
        url = "https://openapi.naver.com/v1/search/shop.json?query=" + encText + '&display=' + disText# JSON 결과

        # 첫 id, secret_id 가져옴
        client_id = id_list[id_idx]
        client_secret = secret_list[secret_idx]
        try : # 요청 시도
            request = urllib.request.Request(url)
            request.add_header("X-Naver-Client-Id",client_id)
            request.add_header("X-Naver-Client-Secret",client_secret)
            response = urllib.request.urlopen(request)
            rescode = response.getcode()
        except : # 요청 불가 시
            time.sleep(0.5)
            request = urllib.request.Request(url)
            id_idx += 1 # 다음 id와 secret_id 사용
            secret_idx += 1 
            if id_idx == 5 : # 끝에 도달할 경우 처음부터 시작 (한계치인 25000개를 채우지 않고 그다음 id로 넘어가는 경우 있었음.)
                id_idx,secret_idx = 0,0
            client_id = id_list[id_idx] # 재 요청
            client_secret = secret_list[secret_idx]
            request.add_header("X-Naver-Client-Id",client_id)
            request.add_header("X-Naver-Client-Secret",client_secret)
            response = urllib.request.urlopen(request)
            rescode = response.getcode()
            print(f'change idx : {id_idx}') # 사용중인 n번째 print

        if(rescode==200):
            response_body = response.read()
            # print('Fin')
        else:
            print("Error Code:" + rescode)

        # 데이터 프레임 생성
        df = pd.DataFrame(json.loads(response_body.decode('utf-8'))['items'])
        if len(df) == 0 : # 결과가 없으면 '' 반환
            nm,maker,brand,cat1,cat2,cat3,cat4 = '','','','','','',''
            return nm,maker,brand,cat1,cat2,cat3,cat4,id_idx,secret_idx
        # 10개의 상품만 확인
        df = df[:10]
        
        df.fillna('',inplace = True)
        for idx in df.index :
            if idx == 0 : 
                # 카테고리는 최상단 검색결과의 상품 정보를 가져옴
                cat1 = df.loc[idx,'category1']
                cat2 = df.loc[idx,'category2']
                cat3 = df.loc[idx,'category3']
                cat4 = df.loc[idx,'category4']
                nm = df.loc[idx,'title']
                nm = nm.replace('<b>','').replace('</b>','').strip()

            # 브랜드와 제조사 정보는 다른 방법으로 차용
            brand = df.loc[idx,'brand']
            maker = df.loc[idx,'maker']
            
            # 1) 브랜드와 제조사가 동시에 있는 경우, 해당 정보 사용
            if (brand != '') & (maker != '') :
                break
            else : # 2) 동시에 있는 값이 없으 공백을 제외한 가장 많이 등장한 브랜드, 제조사 사용
                if (maker != '') :
                    if df['maker'].value_counts().index[0] != '' :
                        maker = df['maker'].value_counts().index[0]
                    else :
                        maker = df['maker'].value_counts().index[1]
                if (brand != '') :
                    if df['brand'].value_counts().index[0] != '' :
                        brand = df['brand'].value_counts().index[0]
                    else :
                        brand = df['brand'].value_counts().index[1]
                        

        return nm,maker,brand,cat1,cat2,cat3,cat4,id_idx,secret_idx
    
    id_idx = 0
    secret_idx = 0
    final_df = pd.DataFrame()
    for prod in tqdm(prdt) :
        nm,maker,brand,cat1,cat2,cat3,cat4,id_idx,secret_idx = shopping_api(prod,id_idx,secret_idx)
                                                                    # 네이버 api에 prod값을 검색한 결과값을 할당
        id_nums = prdt_dic[prod] 
            # 쇼핑 API 결과와 itemId 결합
        result = pd.DataFrame([[prod,id_nums,maker,brand,cat1,cat2,cat3,cat4,nm]], columns = ['prdt_nm','ITEMID','maker','brand','cat1','cat2','cat3','cat4','api_prdt_nm'])
        final_df = pd.concat([final_df,result]).reset_index(drop = True)

    df_drop = df[['PRDT_NM','itemId']].drop_duplicates(['PRDT_NM','itemId'])
    df_drop = df_drop.reset_index(drop = True) # 혹시나 중복이 있을 경우 제거

    md = pd.merge(final_df,df_drop, left_on = 'ITEMID', right_on = 'itemId', how = 'left')

    md = md[['PRDT_NM', 'ITEMID', 'maker', 'brand', 'cat1', 'cat2', 'cat3', 'cat4',
        'api_prdt_nm', 'prdt_nm']]
    md['DATE'] = datetime.now().strftime('%Y-%m-%d') # 수집 당시 날짜 기입 -- 해당 컬럼 값으로 파일 관리

    md.rename(columns = {'PRDT_NM':'PRDT_NM(ORIGIN)'},inplace = True)

    api_df_update = pd.concat([api_df,md]).reset_index(drop = True)
    api_df_update = api_df_update.drop_duplicates(['prdt_nm','ITEMID'])
    api_df_update = api_df_update.reset_index(drop = True)

    # now = datetime.now()
    # dt = now - timedelta(days = 1)

    file_name = f"./api_결과/API_취합.csv"
    api_df_update.to_csv(file_name,index=False, encoding = 'utf-8-sig')
