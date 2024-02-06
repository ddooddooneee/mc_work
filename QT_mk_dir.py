# 필요한 기본적인 모듈 import 
import os
import shutil
import warnings
warnings.filterwarnings('ignore')

# 함수1. input할 파일이 들어갈 폴더를 생성
def madic():
    # 입력 파일이 들어갈 폴더 생성
    input_folder_path = './input_folder'
    if os.path.exists(input_folder_path):
        shutil.rmtree(input_folder_path)
    os.makedirs(input_folder_path, exist_ok=True)

    # 출력 파일이 들어갈 폴더 생성
    output_folder_path = './output_folder'
    if os.path.exists(output_folder_path):
        shutil.rmtree(output_folder_path)
    os.makedirs(output_folder_path, exist_ok=True)
