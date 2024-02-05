# DB에서 리포트 다운 받는 코드 (클래스화)

from dateutil.parser import parse
from tqdm.auto import tqdm
from datetime import datetime, timedelta
import datetime
import pandas as pd
from sqlalchemy import create_engine
from dateutil.parser import parse
from tqdm.auto import tqdm


class make_report :
    start_date = ''
    end_date = ''

    def __init__(self, start_date,end_date) :
        self.start_date = start_date
        self.end_date = end_date

        db_host = ''
        db_port = ''
        db_user = ''
        db_password = ''
        db_database = ''
        engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_database}')
            # sqlalchemy 이용한 db 접속 쿼리

        total_df = pd.DataFrame()
        markets = pd.read_sql('select * from market where main = 1', engine)
        markets = markets[1:11].reset_index(drop = True)
          # markets에 market 테이블에 있는 마켓 고유넘버와 이름이 담기게 됨
        
        def get_week_no(d):
            target = datetime.datetime.strptime(d, '%Y-%m-%d')
            firstday = target.replace(month=1, day=1)
            if firstday.weekday() == 6:
                origin = firstday
            elif firstday.weekday() < 3:
                origin = firstday - datetime.timedelta(days=firstday.weekday() + 1)
            else:
                origin = firstday + datetime.timedelta(days=6 - firstday.weekday())
            return (target - origin).days // 7 + 1


        # 리포트 추출 (1일에 약 50초)
        for idx in tqdm(markets.index) :
            market_id = markets.loc[idx,'marketId']
            market_name = markets.loc[idx,'name']
            for date in pd.date_range(self.start_date, self.end_date) :
                date = date.strftime('%Y-%m-%d')
                date_ = parse(date)
                date_y = date_.strftime('%Y')
                date_m = date_.strftime('%m')
                date_d = date_.strftime('%d')
                date_w = date_.weekday()
                date_n = get_week_no(date_.strftime('%Y-%m-%d'))
                # 여기서 추출, 리포트 추출 시 categoryGroup에 categoryId - categoryItem{marketId} - itemId JOIN
                res = pd.read_sql(f"SELECT * FROM reportData{market_id} WHERE DATE(`date`) = '{date}' AND SALE_QT != 0 AND itemId IN \
                                    (SELECT itemId FROM categoryItem{market_id} ci WHERE ci.categoryId IN (SELECT categoryId FROM categoryGroupCategory cgc WHERE cgc.categoryGroupId = 11)) ORDER BY `date`", engine)
                                        '''
                                            reportData{market_id}에서 모든 열을 추출해오는 쿼리
                                                조건 1. date 값이 start_date, end_date와 동일
                                                조건 2. SALE_QT 값이 0이 아님
                                                조건 3. itemId가 다음과 같은 조건을 만족
                                                    조건 3.1. itemId가 categoryId 테이블에 존재할 것
                                                    조건 3.2. 해당 categoryId가 categoryGroupCategory테이블에서 categoryGroupId가 11인 categoryId 일 
                                        '''
                res['EC_INFO'] = market_name
                res['Y'] = date_y
                res['M'] = date_m
                res['D'] = date_d
                res['W'] = date_n
                res['DAYS'] = date_w

                total_df = pd.concat([total_df,res])

            total_df = total_df.reset_index(drop = True)
        total_df['TIME'] = total_df['date'].apply(lambda x: x.strftime('%H%M%S'))


        total_df = total_df[['Y', 'M', 'D', 'W', 'DAYS', 'TIME', 'itemNumChangeId', 'itemId', 'itemDetailId', 'PRDT_NM', 'PRDT_OPTION', 'PRDT_DE_OPTION', 'PRDT_PRICE', 'PRDT_PRICE_DISCOUNTED', 'SKU_ORG',
                            'P_INT_LEN', 'P_LEN', 'P_INT_WEI', 'P_WEI', 'P_INT_VOL', 'P_VOL', 'P_INT_GB', 'P_GB', 'P_INT_QT', 'P_QT',
                            'PER_PRDT', 'RANK', 'SALE_QT', 'SALE_QT_BEFORE', 'SALE_QT_AFTER', 'PRED_SALES', 'EC_CAT1', 'EC_CAT2', 'EC_CAT3', 'EC_CAT4', 'EC_CAT5', 'EC_BD', 'EC_SELLER_NAME', 'EC_SELLER_CODE',
                            'EC_BUSINESS_NAME', 'EC_BUSINESS_NUMBER','EC_DELIVERY_TYPE', 'EC_INFO', 'NV_CAT1', 'NV_CAT2', 'NV_CAT3', 'NV_CAT4', 'NV_CAT5', 'NV_CATID', 'NV_BD', 'NV_MF', 'URL']]
        
        if self.start_date == self.end_date :
            total_df.to_csv(f'./리포트_도출결과/{self.start_date}_냉세에_리포트.csv', index = False, encoding = 'utf-8-sig')
        
        else :
            total_df.to_csv(f'./리포트_도출결과/{self.start_date}_{self.end_date}_냉세에_리포트.csv', index = False, encoding = 'utf-8-sig')
    
    
