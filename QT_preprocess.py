from sklearn.ensemble import IsolationForest
import pandas as pd
import numpy as np

'''
(SALE_QT_AFTER, SALE_QT_BEFORE)의 기울기 값들을 표본으로 하여 이상치 탐지

  가정1. 정상적인 판매 패턴이라면 기울기 값이 1에서 크게 벗어나지 않을 것.
  가정2. 재고수량이 10개일 때와 100개일 때, 1개가 팔렸을 때의 기울기 값은 매우 다름 
          => 기존 재고수량의 개수(자리수)를 고려하여 이상치를 정제할 필요가 있음.
'''
# 2차 정제 (대상범위: 전체 SALE_QT)
def apply_CTIF(df):
    ## 기울기열 계산
    x=df['SALE_QT_AFTER'].values
    y=df['SALE_QT_BEFORE'].values

    try:
        slopes = y / x
    except ZeroDivisionError:
        slopes=0

    df['SLOPE']=slopes
    df['SLOPE'].replace([np.inf, -np.inf], 0, inplace=True)

    ## 자리수별로 그룹을 나눠서 계산
    df['SALE_CT']=df['SALE_QT_AFTER'].abs().astype(str).apply(len)
        # SALE_QT의 자릿수 계산 EX) 백의자리 => SALE_CT=3
    CT_list=df['SALE_CT'].unique().tolist()
    df_label=pd.DataFrame()

    ## 자리수별로 이상치 탐지 실
    for ct in CT_list:
        df_ctg=df[df['SALE_CT']==ct]
        df_test=df_ctg[['SLOPE']]

        iForest = IsolationForest(  n_estimators = 100, # 나무의 개수
                                    contamination = 'auto', # 전체에서 지정된 이상치 비율
                                    max_samples = 'auto', # 각 나무 훈련시 샘플링하는 개수
                                    bootstrap = False, # 각 나무 훈련을 위한 샘플링 시 True면 복원추출, False면 비복원추출
                                    max_features = 1, # 각 나무 훈련 시 랜덤 선택하는 피쳐의 개수. 디폴트는 1개씩
                                    random_state = 42 )
        iForest.fit(df_test)

        y_pred = iForest.predict(df_test)
        y_score = iForest.score_samples(df_test)
        df_test['anomaly_label']= y_pred
        df_test['label_score'] = y_score

        df_ctg['anomaly_label'] = df_test['anomaly_label'].copy()
        df_ctg['label_score'] = df_test['label_score'].copy()
        
        df_ctg['SL_SALE_QT']=df_ctg['AUTO_IF_SALE_QT'].copy()
        df_ctg.loc[df_ctg['anomaly_label'] == -1, 'SL_SALE_QT'] = 0

        df_label=pd.concat([df_label,df_ctg])

    return df_label


# 3차 정제 (대상범위: AUTO값과 CUSTOM값에서 이상치 범위가 차이가 나는 SALE_QT을 대상으로) 
def apply_CTIF2(df):
    df = df.reset_index()
    df['HM_SALE_QT']=df['CUS_IF_SALE_QT'].copy()
    
    df.loc[(df['CUS_anomaly_label']== -1) & (df['AUTO_anomaly_label']== 1), 'HM_SALE_QT'] = 'idk'
          # CUSTOM값에서는 이상치로 탐지되었으나 AUTO값에서는 정상치로 탐지된 행을 대상으로 재차 이상치 정제
    idk=df[df['HM_SALE_QT']=='idk']

    CT_list2=idk['SALE_CT'].unique().tolist()
    # df_label2=pd.DataFrame()

    for ct in CT_list2:
        df_ctg=idk[idk['SALE_CT']==ct]
        df_test=df_ctg[['SLOPE']]

        iForest = IsolationForest(  n_estimators = 100, # 나무의 개수
                                    contamination = 'auto', # 전체에서 지정된 이상치 비율
                                    max_samples = 'auto', # 각 나무 훈련시 샘플링하는 개수
                                    bootstrap = False, # 각 나무 훈련을 위한 샘플링 시 True면 복원추출, False면 비복원추출
                                    max_features = 1, # 각 나무 훈련 시 랜덤 선택하는 피쳐의 개수. 디폴트는 1개씩
                                    random_state = 42 )
        iForest.fit(df_test)

        y_pred = iForest.predict(df_test)
        y_score = iForest.score_samples(df_test)
        df_test['anomaly_label2']= y_pred
        df_test['label_score2'] = y_score
        
        df_ctg['anomaly_label2'] = df_test['anomaly_label2'].copy()
        df_ctg['label_score2'] = df_test['label_score2'].copy()
        
        df_ctg.loc[df_ctg['anomaly_label2'] == -1, 'HM_SALE_QT'] = 0
        df_ctg.loc[df_ctg['anomaly_label2'] == 1, 'HM_SALE_QT'] = df_ctg['SALE_QT']

        # df_label2 = pd.concat([df_label2, df_ctg])
        
        for idx in df_ctg.index:
            if idx in df.index:
                df.loc[idx, 'HM_SALE_QT'] = df_ctg.loc[idx, 'HM_SALE_QT']

    return df
