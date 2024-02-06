from make_report import *
from daily_shopping_api import *
import sys
from datetime import datetime,timedelta
import qtclean as qc
import dataclean as dc
from summary import *
from dateutil.relativedelta import relativedelta
import file_upload as fu

# 수집 날짜 기록
now = datetime.now()
start_date = now - timedelta(days = 1) # 조회일 - 1
end_date = now - timedelta(days = 1) # 조회일 - 1
start_date = start_date.strftime('%Y-%m-%d')
end_date = end_date.strftime('%Y-%m-%d') # start_date, end_date 같은 날짜 도출


print(f'{start_date} 냉/세/에 리포트 데이터 추출')
make_report(start_date, end_date) # start_end_date == end_date

print('shopping api 수행')
daily_api_run(start_date,end_date,end_date) 

print('수집, api 결합')
# api 파일과 수집데이터 리포트 파일 결합 -> 이후 데이터 정제 수행
# api_df = pd.read_csv('./api_결과/' + os.listdir('./api_결과')[-1], low_memory = False)
api_df = pd.read_csv('./api_결과/API_취합.csv', low_memory = False)
raw_df = pd.read_csv(f'./리포트_도출결과/{end_date}_냉세에_리포트.csv', low_memory = False)
# null 값 제외 후 중복 제거, 원 파일에 null 값 남기는 이유는 추가 수집이 되는 지 test 용도로 사용할 수 있으므로 남김.
not_null_api_df = api_df[~api_df['cat1'].isnull()].reset_index(drop = True)
not_null_api_df['PRDT_NM(ORIGIN)'].fillna(not_null_api_df['prdt_nm'], inplace=True) # 20일 이전은 전부 원본 상품명으로 수집함. 결합을 위해 채움
not_null_api_df.drop_duplicates('PRDT_NM(ORIGIN)', inplace = True) # 원본 상품명 중복 제거

print('결합 (수집 데이터 상품명 <-> 원본 상품명)')
cat_merge = pd.merge(raw_df,not_null_api_df, how = 'left', left_on = 'PRDT_NM', right_on = 'PRDT_NM(ORIGIN)')

cat_merge = cat_merge[['Y', 'M', 'D','W', 'TIME', 'itemNumChangeId', 'itemId',
'itemDetailId', 'PRDT_NM', 'PRDT_OPTION', 'PRDT_DE_OPTION',
'PRDT_PRICE', 'PRDT_PRICE_DISCOUNTED', 'SKU_ORG', 'P_INT_LEN', 'P_LEN',
'P_INT_WEI', 'P_WEI', 'P_INT_VOL', 'P_VOL', 'P_INT_GB', 'P_GB',
'P_INT_QT', 'P_QT', 'PER_PRDT', 'RANK', 'SALE_QT', 'SALE_QT_BEFORE',
'SALE_QT_AFTER', 'PRED_SALES', 'EC_CAT1', 'EC_CAT2', 'EC_CAT3',
'EC_CAT4', 'EC_CAT5', 'EC_BD', 'EC_SELLER_NAME', 'EC_SELLER_CODE',
'EC_BUSINESS_NAME', 'EC_BUSINESS_NUMBER', 'EC_DELIVERY_TYPE', 'EC_INFO',
'NV_CAT1', 'NV_CAT2', 'NV_CAT3', 'NV_CAT4', 'NV_CAT5', 'NV_CATID',
'NV_BD', 'NV_MF', 'URL','maker', 'brand', 'cat1','cat2','cat3', 'cat4','DAYS']] ## 기존 (cat2 제외하고 df 불러옴) >> 수정(cat2 포함해서 df 불러옴)

cat_merge['cat3'] = cat_merge['cat3'].apply(lambda x: str(x).replace('전용냉장고','냉장고')) # 전용냉장고 -> 냉장고로 치환
cat_merge.loc[cat_merge[cat_merge['cat3']=='nan'].index,'cat3'] = np.nan  # 공백 치환
cat_merge.rename(columns = {'cat1':'NEW_CAT','cat2' : 'NEW_CAT1','cat3' : 'NEW_CAT2', 'cat4' : 'NEW_CAT3'},inplace = True) 
                              ## 기존 (NEW_CAT2까지 존재) >> 수정(cat2 포함해서 NEW_CAT3까지 열 추가)
api_file_name = f'./api_merge_data/{start_date}_api_결합데이터.csv' # 파일 저장
cat_merge.to_csv(api_file_name, index = False, encoding = 'utf-8-sig')


print('train data 불러오는 중')
qt_df = pd.read_csv('./3개월치_데이터_저장용/train_data.csv',low_memory = False)

if max(qt_df['DATE']) != start_date : # 3개월 치 데이터의 가장 최근 날짜와 수집 날짜가 맞지 않을 경우
  # 이전의 3개월치와 오늘 수집한 데이터 결합
  total = pd.concat([qt_df.reset_index(drop = True),cat_merge]).reset_index(drop = True)
  total['DATE']= pd.to_datetime(total['Y'].astype(str) + '-' + total['M'].astype(str) + '-' + total['D'].astype(str))
  total['NEW_CAT'] = total['NEW_CAT'].apply(lambda x: str(x).replace('/',''))
  total['NEW_CAT1'] = total['NEW_CAT1'].apply(lambda x: str(x).replace('/',''))
  total['NEW_CAT2'] = total['NEW_CAT2'].apply(lambda x: str(x).replace('/',''))
  total = total[total['NEW_CAT'] == '디지털가전'].reset_index(drop = True) ## 디지털가전만 취급, 카테고리 추가 시 작업 필요 ##

  # 3개월 치 데이터 갱신
  target_dt = datetime.strptime(start_date,'%Y-%m-%d') # start_date = datetime.now() - 1
  dif_3m = relativedelta(months=3) 
  start_dt = target_dt - dif_3m
  total['DATE']= pd.to_datetime(total['Y'].astype(str) + '-' + total['M'].astype(str) + '-' + total['D'].astype(str))
  total = total[(total['DATE'] >= start_dt) & (total['DATE'] <= target_dt)] # 데이터 필터, 3개월 전 ~ 가장 최근
  print('3개월 치 데이터 갱신')
  total.to_csv('./3개월치_데이터_저장용/train_data.csv', index = False, encoding = 'utf-8-sig') # 덮어쓰기


# 이미 한번 실행한 적 있을 경우 같은 날 중복 concat 방지, D-1 날짜와 3개월 치 데이터의 최근 날짜가 같은 경우
else :
  pass

print('QT 정제')
qt_file_name = f'./qt_clean/{start_date}_QT정제.csv'
qc.qtcleanfile('./3개월치_데이터_저장용/train_data.csv',qt_file_name,today = start_date)

print('데이터 정제(브랜드, 상품명)')
clean_data_file_name = f'./clean_data/{start_date}.xlsx'
dc.cleanfile(qt_file_name,clean_data_file_name)

# if os.path.isfile(qt_file_name):
#   os.remove(qt_file_name)

print('데이터 집계')
# 최종 데이터 'Y-M-D_sales_data.xlsx' 저장 (아직 폴더 생성 X)
df_summary(clean_data_file_name, end_date)


# 데이터 업로드
fu.requests_api(end_date) 
