import requests

def requests_api(date) : # 수집 진행한 날짜를 기입해야 함
    '''
    개발팀 운영 DB에 업로드할 때 쓰는 코드
    '''

    # 업로드 데이터 경로
    file_path = f'./Report_data/{date}_sales_data.xlsx'

    # 업로드할 URL
    upload_url = 'http://192.168.0.99:7203/batch/upload-file'

    # 업로드 준비
    files = {'file': open(file_path, 'rb')}

    # POST 요청
    response = requests.post(upload_url, files=files)

    # 응답 확인
    print(response.status_code)
    print(response.text)
    print('종료')
    
