from sklearn.ensemble import IsolationForest
import pandas as pd
import numpy as np

'''
SALE_QT 이상치 탐지의 가장 기본 모델 :
  contamination값을 'auto'와 '0.1' 또는 임의의 지정한 값으로 돌릴 수 있도록 하는 코드
'''


# 함수 2. 이상치 탐지하는 함수
        ## cotamination 기본값=auto 이지만 변경할 수 있게 수정
def apply_IF(df,contam=0.1):
    # AUTO값으로 정제
    df_test=df[['RANK','SALE_QT']]
                # 'RANK'와 'SALE_QT' 두 열이 이상치와 가장 연관성이 높은 것으로 판정
    iForest = IsolationForest(n_estimators = 100,
                             contamination = 'auto',
                             max_samples = 'auto',
                             bootstrap = False,
                             max_features = 1,
                             random_state = 42)
    iForest.fit(df_test)

    # 이상치값을 열에 추가
    y_pred = iForest.predict(df_test)
    y_score = iForest.score_samples(df_test)
    df_test['anomaly_label']= y_pred
    df_test['anomaly_score'] = y_score

    # 원래 데이터프레임에 해당 열의 값을 추가 'IF_SALE_QT'라는 새로운 열의 값 생성
    df['AUTO_anomaly_label'] = df_test['anomaly_label'].copy()
    df['AUTO_IF_SALE_QT'] = df_test['SALE_QT'].copy()

    df.loc[df['AUTO_anomaly_label'] == -1, 'AUTO_IF_SALE_QT'] = 0


    # 함수에서 지정한 값으로 정제 : 기본값 0.1     
    df_test=df[['RANK','SALE_QT']]
    iForest = IsolationForest(n_estimators = 100,
                             contamination = contam,
                             max_samples = 'auto',
                             bootstrap = False,
                             max_features = 1,
                             random_state = 42)
    iForest.fit(df_test)

    # 이상치값을 열에 추가
    y_pred = iForest.predict(df_test)
    y_score = iForest.score_samples(df_test)
    df_test['anomaly_label']= y_pred
    df_test['anomaly_score'] = y_score

    # 원래 데이터프레임에 해달 열의 값을 추가 'IF_SALE_QT'라는 새로운 열의 값 생성
    df['CUS_anomaly_label'] = df_test['anomaly_label'].copy()
    df['CUS_IF_SALE_QT']=df_test['SALE_QT'].copy()

    df.loc[df['CUS_anomaly_label'] == -1, 'CUS_IF_SALE_QT'] = 0
    
    return df
