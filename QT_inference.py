from mk_dir import *
from preprocess import *
from model import *

'''
기존에 만들어 놓은 QT정제 모듈(QT_preprocess.py, QT_model.py)를 기반으로 SALE_QT 값의 이상치 정제하는 모듈
  : 파일 READ ~ 이상치 정제 ~ 파일 저장까지 코드 실행 함수
'''

def cleanQT(ctg,ctg_name,contam=0.1) :
    path='./input_folder'
    file_list = [] # csv 파일 목록

    # 디렉토리 내의 모든 파일 검색
    for name in os.listdir(path):
        if name.endswith('.csv'):
            file_list.append(os.path.join(path, name))
    # CSV 파일을 데이터프레임으로 로드하고 이를 리스트에 저장 
    
    # 카테고리 리스트 
    ctg_list=['NEW_CAT','NEW_CAT1','NEW_CAT2','NEW_CAT3','NEW_CAT4']

    for file in file_list: 
        df = pd.read_csv(file)
        df_ctg=df[df[ctg_list[ctg-1]]==ctg_name]
          ## 만약 최상위 또는 최하위 카테고리라면 error 패스하도록 코드 수정하기
        df_name_list=[ctg_name]

        # 하위 카테고리에 대한 리스트 제작
        df_ctg_list=df_ctg[ctg_list[ctg]].dropna().unique().tolist()
        df_name_list=df_name_list+df_ctg_list
        df_name_list = [value for value in df_name_list if pd.notnull(value)]

        final_df = pd.DataFrame()

        for tf in df_name_list:
            # 카테고리별로 이상치 정제 반복
            if tf==df_name_list[0]: # 가장 상위 카테고리
                # print(tf)  
                df_tf=df_ctg
                # 1차 정제 : 카테고리별로 Isolation Forest
                df_tf = apply_IF(df_tf,contam)
                # 2차 정제 : 기울기값들의 이상치를 Isolation Forest
                df_tf = apply_CTIF(df_tf)
                # 3차 정제 : 
                df_tf = apply_CTIF2(df_tf)
                # 파이널 SALE_QT
                df_tf["NEW_SALE_QT"] = df_tf.apply(lambda row: row['HM_SALE_QT'] if row['SALE_QT'] > 0 else row['SL_SALE_QT'], axis=1)
            
            elif tf!=df_name_list[0]: # 그다음 하위카테고리
                try :
                    # print(tf)
                    df_tf=df_ctg[df_ctg[ctg_list[ctg]]==tf]

                    # 1차 정제 : 카테고리별로 Isolation Forest
                    df_tf = apply_IF(df_tf,contam)
                    # 2차 정제 : 기울기값들의 이상치를 Isolation Forest
                    df_tf = apply_CTIF(df_tf)
                    # 3차 정제 : 
                    df_tf = apply_CTIF2(df_tf)
                    # 파이널 SALE_QT
                    df_tf["NEW_SALE_QT"] = df_tf.apply(lambda row: row['HM_SALE_QT'] if row['SALE_QT'] > 0 else row['SL_SALE_QT'], axis=1)

                except:
                    print(f"경고:카테고리에 해당하는 데이터가 없습니다.")
                    break

            min_max_values = {
                'NEW_min': df_tf['NEW_SALE_QT'].min(),
                'NEW_max': df_tf['NEW_SALE_QT'].max(),
                'NEW_sum': df_tf['NEW_SALE_QT'].sum()
            }
            final = pd.DataFrame(min_max_values, index=[tf])
            final_df = pd.concat([final, final_df])

            # 최종 파일 저장
            drop_column_list=['AUTO_anomaly_label','AUTO_IF_SALE_QT','CUS_anomaly_label','CUS_IF_SALE_QT','SLOPE','SALE_CT','anomaly_label',
                              'label_score','SL_SALE_QT','HM_SALE_QT','index']
            df_tf.drop(drop_column_list, axis=1, inplace=True)

            name= f'./output_folder/{tf}_QT수정ver.csv'
            df_tf.to_csv(name,index=False,encoding='utf-8-sig')
            print(f'{tf}파일 저장완료')

        # 결과 데이터프레임 출력
        df_name = f'./output_folder/최종 QT 결과_{ctg_name}.csv'
        final_df.to_csv(df_name,encoding='utf-8-sig')
        print('최종 결과 파일 저장완료')

        return final_df
    
