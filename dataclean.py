import pandas as pd
import numpy as np
import re

def cleanfile(input_file_path,output_file_path):

    # 데이터 정제할 파일 read
    df=pd.read_csv(input_file_path, encoding='utf-8-sig')

    # 브랜드 딕셔너리 
    bd_csv=pd.read_csv('./brand_keyword/가전_브랜드키워드.csv',encoding='utf-8',header=None)
    bd_df=pd.read_csv('./brand_keyword/가전_브랜드+제조사.csv', header=None)


    # 1. 브랜드 추가
    bd=bd_csv[0] # bd_csv = 가전_브랜드키워드
    bd_list=bd.values.tolist() # 선택한 특정 열의 값을 추출후 리스트에 담기

    df['BRAND']='' # 브랜드이름을 추가할 열

    # 브랜드리스트와 제품이름에 일치하는 요소가 있다면 브랜드열에 추가
    df['PR_NM_UPPER']=df['PRDT_NM'].apply(lambda x:x.upper())

    for brand in bd_list:
        matching_rows = df[df['PR_NM_UPPER'].str.contains(brand,na = False)].index
        df.loc[matching_rows, 'BRAND'] = brand

    df.drop(columns=['PR_NM_UPPER'], inplace=True)

    bd_dict={} # 제조사 딕셔너리
    # 각 열들을 리스트로 만든 후 딕셔너리로 만듬
    ## bd_df = 가전_브랜드_제조사
    for index, row in bd_df.iterrows():
        list_name=row[1]
        list_items=row[2:].tolist()

        cleaned_list_items = [item for item in list_items if pd.notna(item)]
        bd_dict[list_name] = cleaned_list_items

    # 딕셔너리의 각 키값의 value값 리스트와 브랜드명을 비교
    for index, row in df.iterrows():
        bd_value = row['BRAND']
        for key, values in bd_dict.items():
            if any(value in bd_value for value in values):
                df.at[index, 'BRAND'] = key  # 'CAT1' 열 값 설정
                break

    # brand열의 nan 값을 대체 후에 다시 재정리
    df['brand'].fillna(df['BRAND'], inplace=True)
          # brand(API)기준에서 붙지 않은 상품들은 BRAND(딕셔너리)기준의 값을 가져와 쓴다.
    df['brand'] = df['brand'].apply(lambda x: '' if x == 'UNKNOWN' else x)
        # brand값 중에서 UNKNOWN 값은 NAN값과 다름 없기 떄문에 이를 빈 값으로 대체한다.

    for index, row in df.iterrows():
        bd_value = row['brand']
        for key, values in bd_dict.items():
            if any(value in bd_value for value in values):
                df.at[index, 'brand'] = key  # 'CAT1' 열 값 설정
                break

    # brand열의 빈값('')을 maker 값으로 대체
    df['maker'] = df['maker'].apply(lambda x: '' if x == 'UNKNOWN' else x)
    df['brand'] = df.apply(lambda row: row['maker'] if row['brand'] == '' else row['brand'], axis=1)

    # 불필요한 열정리
    df.drop(columns=['BRAND'], inplace=True)
    df.rename(columns={'brand':'BRAND', 'itemId': 'ITEMID', 'itemDetailId': 'ITEMDETAILID'},inplace=True)



    # 2. 모델명 추가
    def md_maker(df):
        md=pd.read_excel('./brand_keyword/냉세에_삼성LG위니아.xlsx')

        md_list = md['모델명'].to_list()
        
        for md_col in md_list:
            # 'PRDT_NM'열에서 모델명 우선 추론
            matching_rows1 = df[df['PRDT_NM'].str.contains(md_col, na=False)].index
            df.loc[matching_rows1, '모델명'] = md_col

        for md_col in md_list:
            # 'PRDT_OPTION'열에서 모델명 재차 추론
            matching_rows2 = df[(df['모델명'] == '') & df['PRDT_OPTION'].str.contains(md_col, na=False)].index
            df.loc[matching_rows2, '모델명'] = md_col

        for md_col in md_list:
            # 'PRDT_DE_OPTION'열에서 모델명 최종 추론
            matching_rows3 = df[(df['모델명'] == '') & df['PRDT_DE_OPTION'].str.contains(md_col, na=False)].index
            df.loc[matching_rows3, '모델명'] = md_col
        
        df['모델명'].fillna('', inplace=True)
        df['모델명'] = df['모델명'].replace('nan','') # 이 부분 추가
        return df

    def md_clean(df):
        # 제품명 정제할 새로운 열 제작
        df['PR_NM_UPPER']=df['PRDT_NM'].copy()
        df['PR_OPTION_UPPER']=df['PRDT_OPTION'].copy()
        df['PR_DE_OPTION_UPPER'] = df['PRDT_DE_OPTION'].copy()

        # 결측값 대체
        df['PR_NM_UPPER'].fillna(df['PR_OPTION_UPPER'], inplace=True)
        df['PR_OPTION_UPPER'].fillna(df['PR_NM_UPPER'], inplace=True)

        md_col1 = ['PR_NM_UPPER', 'PR_OPTION_UPPER', 'PR_DE_OPTION_UPPER']

        for col in md_col1:
            df[col].fillna('', inplace=True)  # null 값을 빈 문자열로 대체
            df[col] = df[col].apply(lambda x:x.upper())  # 상품명에 들어간 영문을 모두 대문자로 치환
            df[col] = df[col].apply(lambda x:re.sub(r'[^\w\s,\+\-]', ' ', x)) # 알파벳, 공백, 쉼표(,), 플러스(+), 마이너스(-) 문자 이외의 모든 문자 공백으로 대체
            df[col] = df[col].apply(lambda x:re.sub(r'[,_]', ' ', x))

        # 모델명을 정제할 새로운 열 생성
        df['MD_NM'] = df['PR_NM_UPPER'].copy()
        df['MD_OPTION'] = df['PR_OPTION_UPPER'].copy()
        df['MD_DE_OPTION'] = df['PR_DE_OPTION_UPPER'].copy()

        # 작업용 행 제거
        df.drop(columns=['PR_NM_UPPER', 'PR_OPTION_UPPER', 'PR_DE_OPTION_UPPER'], inplace=True)

        # 모델명 정제
        def clean_model(model):
            model = re.sub(r'[^-a-zA-Z0-9\s+]', '', re.sub('[ㄱ-ㅎㅏ-ㅣ가-힣]', '', model))  # 한글 제거(알파벳, 숫자, 공백, 하이픈 제외)
            model = re.sub(r'\s+', ' ', model)  # 연속된 공백을 단일 공백으로 변경
            model = ' '.join([word.strip('-') for word in model.split()])  # 시작과 끝의 하이픈 제거
            return model

        # 모델명 필터링 (조건 : 두글자 이상의 알파벳과 두글자 이상의 숫자 조합)
        def filter_model(model):
            words = model.split()
            filtered_words = [word for word in words if sum(char.isalpha() for char in word) >= 2 and sum(char.isdigit() for char in word) >= 2]
            return ' '.join(filtered_words)

        md_col2 = ['MD_NM', 'MD_OPTION', 'MD_DE_OPTION']
        
        for col in md_col2:
            df[col] = df[col].apply(clean_model) # 모델명 정제
            df[col] = df[col].apply(filter_model) # 모델명 필터링

        # ☆세트상품☆ - 세트 모델명을 정제할 새로운 열 생성
        df['SET_NM'] = df['MD_NM'].copy()
        df['SET_OPTION'] = df['MD_OPTION'].copy()
        df['SET_DE_OPTION'] = df['MD_DE_OPTION'].copy()

        def set_prdt(model):
            model = re.sub(r'[0-9]{1,3}(KG|MM)[+][0-9]{1,3}(KG|MM)', '', model) # 예시: 1KG+2MM', '123KG+456MM', '99+3MM' -> 상품 규격 표현 제거
            model = re.findall(r'([A-Z0-9]{1,20}\+[A-Z0-9]{1,20}){1,3}', model) # 알파벳 or 숫자로 구성된 문자열과 + 기호의 조합 찾기, '+[A-Z0-9]{1,20}' 패턴 1에서 3번 반복 가능
            return '+'.join(model) if model else ''

        # 세트상품 모델명 필터링 (조건 : 두글자 이상의 알파벳과 두글자 이상의 숫자 조합)
        def filter_md_set(model):
            set_name = model.split('+')
            set_names = [set for set in set_name if sum(char.isalpha() for char in set) >= 2 and sum(char.isdigit() for char in set) >= 2]
            return '+'.join(set_names)

        set_col = ['SET_NM', 'SET_OPTION', 'SET_DE_OPTION']
        for col in set_col:
            df[col] = df[col].apply(set_prdt)
            df[col] = df[col].apply(filter_md_set) # 세트상품 모델명 필터링
            df[col] = df[col].apply(lambda x: x if '+' in x else '') # + 기호 포함하는 경우만 남음

        df['SET_MODEL'] = df['SET_DE_OPTION']

        # 'SET_MODEL' 컬럼의 공백('')을 'SET_OPTION' 컬럼의 데이터로 채우기
        df['SET_MODEL'] = df.apply(lambda row: row['SET_OPTION'] if row['SET_MODEL'] == '' else row['SET_MODEL'], axis=1)
        df['SET_MODEL'] = df.apply(lambda row: row['SET_NM'] if row['SET_MODEL'] == '' else row['SET_MODEL'], axis=1)

        # 세트상품 끝!
        
        # 휴대폰 기종(아이폰&갤럭시) 불용어 제거
        phone = pd.read_csv('./brand_keyword/휴대폰_아이폰_갤럭시_기종.csv', header=0)
        iphone = set(phone[phone['브랜드'] == '아이폰']['모델명'])
        galaxy = set(phone[phone['브랜드'] == '갤럭시']['모델명'])

        def md_phone(model):
            words = model.split()
            filtered_words = [word for word in words if word not in iphone and word not in galaxy]
            return ' '.join(filtered_words)

        # 모델명 제거 필요한 패턴들
        pattern = [
            r'\b(\d{1,6}(M|MM|CM|GB|ML|KG|IN|SIZE|HZ|MB|PCS|BIT|GBPS|MBPS|KBPS|PK|KW|TB|EA|INCH|MA|MAH|VA|RPM|PA))\b', # 0~000000MM|CM|GB|ML|KG|IN10000
            r'\b\d+IN\d+\b', # 2IN1, 3IN1
            r'\b\d+X\d+(CM|MM)\b', # 숫자X숫자CM|MM
            r'\d+X\d+X\d+(MM|CM)', # 숫자X숫자X숫자MM|CM
            r'\b\d+-\d+(MM|CM|SIZE)\b', # 숫자-숫자MM|CM|SIZE
            r'\b\d+(M|MM|CM)X\d+(M|MM|CM)\b', # 숫자M|CM|MMX숫자M|CM|MM
            r'\d+X\d+X\d+', #숫자X숫자X숫자,
            r'[SLMXL]\d+X\d+MM' #사이즈(S, M, L, XL)숫자X숫자MM
        ]

        # 모든 패턴을 하나로 결합
        patterns = '|'.join(pattern)

        def match_pattern(model): # 패턴 제거
            words = model.split()
            clean_words = [word for word in words if not re.match(patterns, word)]
            return ' '.join(clean_words)

        df['MODEL'] = df['모델명']

        for col in md_col2:
            df[col] = df[col].str.replace('+', ' ') # CRP-P0660FD+12P 예시 때문에 제거 필요
            df[col] = df[col].apply(filter_model) # 모델명 필터링 (조건 : 두글자 이상의 알파벳과 두글자 이상의 숫자 조합)
            df[col] = df[col].apply(md_phone) # 휴대폰 기종(아이폰&갤럭시) 불용어 제거
            df[col] = df[col].apply(match_pattern) # 불필요한 패턴 제거
            df[col] = df[col].str.split().apply(lambda x: ' '.join(set(x))) # 한개의 데이터 내 중복된 요소 제거
        
        # MODEL열을 MD_DE_OPTION, MD_OPTION, MD_NM열로 채우기
        for col in md_col2:
            df['MODEL'] = df.apply(lambda row: row[col] if row['MODEL'] == '' else row['MODEL'], axis=1)

        df['MODEL'] = df.apply(lambda row: row['SET_MODEL'] if row['SET_MODEL'] != '' else row['MODEL'], axis=1)
        
        # 최종 가장 긴 문자열 -> 모델명 결정
        df['MODEL'] = df['MODEL'].astype(str)
        df['MODEL'] = df['MODEL'].str.split().apply(lambda x: max(x, key=len) if x else '')

        # BRAND가 '스타리온'이고, MODEL이 'SR'로 시작하지 않고, 공백이 아닌 MODEL에 'SR-' 추가
        # df.loc[(df['BRAND'] == '스타리온') & (~df['MODEL'].str.startswith('SR')) & (df['MODEL'] != ''), 'MODEL'] = 'SR-' + df['MODEL']
        df.loc[(df['BRAND'] == '스타리온') & (~df['MODEL'].str.startswith('SR')), 'MODEL'] = 'SR-' + df['MODEL']
        return df


    df = md_maker(df) # 모델명 딕셔너리와 비교해서 추가
    df = md_clean(df) # 상품명에서 추론되는 모델명 추가


    # 3. 상품명 정제

    def del_keyword(df):
        # 제품명 정제할 새로운 열 제작
        df['PR_NM_UPPER']=df['PRDT_NM'].copy()

        # 상품명에 들어간 영문을 모두 대문자로 치환
        df['PR_NM_UPPER']=df['PR_NM_UPPER'].apply(lambda x:x.upper())

        # 기타특수문자 제거
        df['PR_NM_UPPER']=df['PR_NM_UPPER'].apply(lambda x:re.sub(r'[^\w\s,\+\-\_\%\.\[\]\(\)]', ' ', x))
        df['PR_NM_UPPER']=df['PR_NM_UPPER'].apply(lambda x:re.sub(r'[\n]','',x))
        

        # 불용어 제거
        ## 불용어 데이터프레임 read
        word_csv=pd.read_csv('./brand_keyword/불용어.csv',header=None)

        df['PRDT_NM_CLEAN']=df['PR_NM_UPPER'].copy()
        
        ## ']' 문자 뒤에 공백 추가
        df['PRDT_NM_CLEAN'] = df['PRDT_NM_CLEAN'].apply(lambda x: re.sub(r'\]', '] ', x))


        ## 불용어 list 제작
        word_list=word_csv[0].values.tolist()

        ## 불용어 제거
        for wd in word_list:
            escaped_wd = re.escape(wd)
            df['PRDT_NM_CLEAN']=df['PRDT_NM_CLEAN'].apply(lambda x:re.sub(escaped_wd,'',x))

        # 왼쪽 첫 문자가 알파벳 하나로만 이루어져 있거나 공백인 경우 해당 문자를 삭제
        df['PRDT_NM_CLEAN'] = df['PRDT_NM_CLEAN'].apply(lambda x: re.sub(r'^(?=[A-Za-z]\s|\s)', '', x))

        # 01. 02. 와 같은 넘버링 삭제
        df['PRDT_NM_CLEAN'] = df['PRDT_NM_CLEAN'].apply(lambda x: re.sub('상품 [0-9]{1,2}.','',x).strip())
        df['PRDT_NM_CLEAN'] = df['PRDT_NM_CLEAN'].apply(lambda x: re.sub(r'\b\d+\.\s*', '', x).strip())

        # []와 ()안에 있는 문자 삭제
        df['PRDT_NM_CLEAN'] = df.apply(lambda row: re.sub(r'\[[^\]]*\]|\((?![가-힣])[^)]*\)', '', row['PRDT_NM_CLEAN'])
                                                    if row['BRAND'] != re.sub(r'\[[^\]]*\]|\((?![가-힣])[^)]*\)', '', row['PRDT_NM_CLEAN'])
                                                    else row['PRDT_NM_CLEAN'], axis=1)
    

        # 공백 정리
        df['PRDT_NM_CLEAN'] = df['PRDT_NM_CLEAN'].apply(lambda x: re.sub(r'\s{2,}', ' ', x))

        # 상품명 시작이 특수문자인 경우 제거
        df['PRDT_NM_CLEAN'] = df['PRDT_NM_CLEAN'].apply(lambda x: re.sub(r'^(?!\()[\W]+', '', x))

        # 상품명 좌우 공백 제거
        df['PRDT_NM_CLEAN'] = df['PRDT_NM_CLEAN'].str.strip()

        return df

    def del_keyword2(df):
        # 제품명 정제할 새로운 열 제작
        df['PR_DE_OPTION']=df['PRDT_OPTION'].copy()


        # 결측값 대체
        df['PR_DE_OPTION'].fillna(df['PR_NM_UPPER'], inplace=True)


        # 상품명에 들어간 영문을 모두 대문자로 치환
        df['PR_DE_OPTION']=df['PR_DE_OPTION'].apply(lambda x:x.upper())

        # 기타특수문자 제거
        df['PR_DE_OPTION']=df['PR_DE_OPTION'].apply(lambda x:re.sub(r'[^\w\s,\+\-\_\%\.\[\]\(\)]', ' ', x))

        df['PR_DE_OPTION']=df['PR_DE_OPTION'].apply(lambda x:re.sub(r'[\n]','',x))
        

        # 불용어 제거
        ## 불용어 데이터프레임 read
        word_csv=pd.read_csv('./brand_keyword/불용어.csv',header=None)

        df['PRDT_OPTION_CLEAN']=df['PR_DE_OPTION'].copy()
        
        ## ']' 문자 뒤에 공백 추가
        df['PRDT_OPTION_CLEAN'] = df['PRDT_OPTION_CLEAN'].apply(lambda x: re.sub(r'\]', '] ', x))


        ## 불용어 list 제작
        word_list=word_csv[0].values.tolist()

        ## 불용어 제거
        for wd in word_list:
            escaped_wd = re.escape(wd)
            df['PRDT_OPTION_CLEAN']=df['PRDT_OPTION_CLEAN'].apply(lambda x:re.sub(escaped_wd,'',x))

        # 왼쪽 첫 문자가 알파벳 하나로만 이루어져 있거나 공백인 경우 해당 문자를 삭제
        df['PRDT_OPTION_CLEAN'] = df['PRDT_OPTION_CLEAN'].apply(lambda x: re.sub(r'^(?=[A-Za-z]\s|\s)', '', x))

        # 01. 02. 와 같은 넘버링 삭제
        df['PRDT_OPTION_CLEAN'] = df['PRDT_OPTION_CLEAN'].apply(lambda x: re.sub('상품 [0-9]{1,2}.','',x).strip())
        df['PRDT_OPTION_CLEAN'] = df['PRDT_OPTION_CLEAN'].apply(lambda x: re.sub(r'\b\d+\.\s*', '', x).strip())

        # []와 ()안에 있는 문자 삭제
        df['PRDT_OPTION_CLEAN'] = df.apply(lambda row: re.sub(r'\[[^\]]*\]|\((?![가-힣])[^)]*\)', '', row['PRDT_OPTION_CLEAN'])
                                                    if row['BRAND'] != re.sub(r'\[[^\]]*\]|\((?![가-힣])[^)]*\)', '', row['PRDT_OPTION_CLEAN'])
                                                    else row['PRDT_OPTION_CLEAN'], axis=1)
    

        # 공백 정리
        df['PRDT_OPTION_CLEAN'] = df['PRDT_OPTION_CLEAN'].apply(lambda x: re.sub(r'\s{2,}', ' ', x))

        # 상품명 시작이 특수문자인 경우 제거
        df['PRDT_OPTION_CLEAN'] = df['PRDT_OPTION_CLEAN'].apply(lambda x: re.sub(r'^(?!\()[\W]+', '', x))

        # 상품명 좌우 공백 제거
        df['PRDT_OPTION_CLEAN'] = df['PRDT_OPTION_CLEAN'].str.strip()

        
        # 작업용 행 제거
        df.drop(columns=['PR_DE_OPTION'], inplace=True)   
        df.drop(columns=['PR_NM_UPPER'], inplace=True)

        return df



    df=del_keyword(df)
    df=del_keyword2(df)

    # 상품명 추가 정제 (옵션 + 상픔명) : 상품명은 같은데 세부옵션만 다른 경우 대비하여 정제추가
    option_delete=['(희망일)','단일상품','폐가전수거있음','폐가전수거없음','요청사항에기재','전국지역','동의','동의함','없음']

    for wd in option_delete:
        escaped_wd = re.escape(wd)
        df['PRDT_OPTION_CLEAN']=df['PRDT_OPTION_CLEAN'].apply(lambda x:re.sub(escaped_wd,'',x))

    df['NEW_PRDT_NM']=''

    df['NEW_PRDT_NM'] = df.apply(lambda row: row['PRDT_NM_CLEAN'] + ' ' +row['PRDT_OPTION_CLEAN'] if bool(re.search('[가-힣/ ]', row['PRDT_OPTION_CLEAN'])) and not re.search(re.escape(row['PRDT_OPTION_CLEAN']), row['PRDT_NM_CLEAN'])
                                                else row['PRDT_NM_CLEAN'] if bool(re.search('[a-zA-Z0-9]', row['PRDT_OPTION_CLEAN'])) or re.search(re.escape(row['PRDT_OPTION_CLEAN']), row['PRDT_NM_CLEAN'])
                                                else row['PRDT_OPTION_CLEAN'], axis=1)
            ''' 
            케이스 1. 상품세부옵션에 한글, 슬래시(/), 혹은 공백이 포함되어 있고, 상품명 값에 상품세부옵션의 내용이 포함되어 있지 않은 경우
                        ⇒ [상품명] + [상품세부옵션]
            케이스 2. 상품 세부 옵션에 영어와 숫자로만 구성 또는 상품명 값에 상품세부옵션 값이 포함되어 있는 경우
                        ⇒ [상품명]
            케이스 3. 그 외의 경우
                        ⇒ [상품 세부 옵션]
            '''


    df['NEW_PRDT_NM'] = df.apply(lambda row: row['PRDT_NM_CLEAN'] if (row['NEW_PRDT_NM'] == '') or (row['NEW_PRDT_NM'] == ' ') else row['NEW_PRDT_NM'], axis=1)


    # 4. 데이터프레임 필요한 열만 추출
    def edit_df(df) :
        df['DATE'] = pd.to_datetime(df[['Y', 'M', 'D']].rename(columns={'Y': 'year', 'M': 'month', 'D': 'day'})).dt.strftime('%Y-%m-%d')
        df = df.drop(['PRDT_NM', 'PRDT_OPTION', 'SALE_QT', 'PRED_SALES'], axis=1)
        df.rename(columns={'PRDT_NM_CLEAN': 'PRDT_NM', 'PRDT_OPTION_CLEAN': 'PRDT_OPTION', 'NEW_SALE_QT': 'SALE_QT', 'PRED_SALES2': 'PRED_SALES'}, inplace=True)
        return df

    df = edit_df(df)



    df[['DATE', 'ITEMID', 'ITEMDETAILID', 'NEW_PRDT_NM','PRDT_NM', 'PRDT_OPTION', 'PRDT_PRICE', 'PRDT_PRICE_DISCOUNTED', 'SALE_QT', 'PRED_SALES', 'EC_CAT1', 'EC_CAT2', 'EC_CAT3', 'EC_CAT4',
        'EC_CAT5', 'NEW_CAT', 'NEW_CAT1', 'NEW_CAT2', 'NEW_CAT3', 'EC_INFO', 'URL', 'BRAND', 'MODEL']].to_excel(output_file_path, index=False)
