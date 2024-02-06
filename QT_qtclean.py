import inference as infe
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os

'''
정의해놓은 모듈을 합해서 최종으로 한번에 실행시키는 모듈
'''

def qtcleanfile(input_file_path,output_file_path, today= '2023-10-21'):
    qt_df =pd.read_csv(input_file_path, encoding='utf-8-sig')
              # 최종 3개월치 거래만 모여있는 파일을 사용할 예정이므로 아래 코드는 전부 제거
    # df['DATE']= pd.to_datetime(df['Y'].astype(str) + '-' + df['M'].astype(str) + '-' + df['D'].astype(str))

    # # 작업파일 : 오늘날짜 기준으로 직전 삼개월
    # target_date = datetime.strptime(today, '%Y-%m-%d')

    # dif_3m = relativedelta(months=3)
    # start_date = target_date - dif_3m

    # qt_df = df[(df['DATE'] >= start_date) & (df['DATE'] <= target_date)]
    
    '''
    start_date = datetime.now() - relativedelta(months=3)
    qt_df = df[(df['DATE'] >= start_date) & (df['DATE'] <= datetime.now())]
    '''


    # 작업환경 세팅
    infe.madic()
    qt_df.to_csv('.\input_folder\데일리QT정제.csv',index=False,)

    # 1차 정제
    infe.cleanQT(1,'디지털가전',0.1) # 카테고리 확장 시 해당 부분 변수처리 필요
    try_once=pd.read_csv("./output_folder/디지털가전_QT수정ver.csv")
    try_once = try_once[try_once['NEW_SALE_QT'] != 0]

    print('1차 정제 완')

    # 2차 정제
    infe.madic()
    try_once.to_csv('./input_folder/0제거.csv', encoding='utf-8-sig',index=False)

    infe.cleanQT(1,'디지털가전',0.1)
    print('2차 정제 완')

    # 결과파일 합치기
    path='./output_folder'
    file_list = [] # csv 파일 목록

    # 디렉토리 내의 모든 파일 검색
    # 디렉토리 내 파일 목록 가져오기
    for name in os.listdir(path):
        if name.endswith('.csv'):
            if '디지털가전' in name or '최종 QT 결과' in name:
                continue
            # 위의 케이스 빼고 나머지는 파일 경로를 리스트에 추가
            file_list.append(os.path.join(path, name))

    # 최종 결과 데이터프레임
    combined_df = pd.DataFrame()

    for file_path in file_list:
        current_df = pd.read_csv(file_path)
        combined_df = pd.concat([combined_df, current_df], ignore_index=True)


    
    # 매출액 계산
    combined_df['PRED_SALES2']= combined_df['PRDT_PRICE'] * combined_df['NEW_SALE_QT']

    
    # 해당 날짜만 추출
    final_df=combined_df[combined_df['DATE']==today]
      # 직전 3개월 데이터에서 데일리 데이터만 추출

    final_df.to_csv(output_file_path,index=False,encoding='utf-8-sig')

