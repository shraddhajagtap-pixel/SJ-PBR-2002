import logging
from logging.handlers import RotatingFileHandler
import os
import requests
import pandas as pd 
from bs4 import BeautifulSoup
from urllib.parse import quote, urljoin
from io import BytesIO
from PIL import Image
import pytesseract
import cv2
import time
from datetime import datetime
import base64

from sqlalchemy import create_engine, text

pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

main_url = "https://rera.punjab.gov.in/reraindex/publicview/projectinfo"

# engine_115 = create_engine("mysql://Shradha:%s@192.168.0.115:3306/punjab_rera" % quote("Efcdata@2025"))

engine_self = create_engine("mysql://Shraddha:%s@localhost:3306/punjab_rera" % quote("Smart@2025"))

path = r'C:\Users\pc\Shraddha\new_env\PunjabRera\HTML_Files'

log_file = r"C:\Users\pc\Shraddha\new_env\PunjabRera\Logs\step1.log"

# remove log file
if os.path.exists(log_file):
    os.remove(log_file)


# Set up rotating log file
handler = RotatingFileHandler(
    log_file,
    maxBytes=5 * 1024 * 1024,  # 5 MB
    backupCount=2 ,
    encoding='utf-8'             # Keep 2 old log files
)

# Format settings
formatter = logging.Formatter(
    fmt="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)
handler.setFormatter(formatter)

# Configure root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.handlers = [handler] 

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
})

districts = [
    {"code": "39", "name": "Amritsar"},
    {"code": "79", "name": "Barnala"},
    {"code": "84", "name": "Bathinda"},
    {"code": "128", "name": "Chandigarh"},
    {"code": "202", "name": "Faridkot"},
    {"code": "204", "name": "Fatehgarh Sahib"},
    {"code": "206", "name": "Fazilka"},
    {"code": "207", "name": "Firozpur"},
    {"code": "232", "name": "Gurdaspur"},
    {"code": "248", "name": "Hoshiarpur"},
    {"code": "261", "name": "Jalandhar"},
    {"code": "300", "name": "Kapurthala"},
    {"code": "366", "name": "Ludhiana"},
    {"code": "2024", "name": "Malerkotla"},
    {"code": "386", "name": "Mansa"},
    {"code": "393", "name": "Moga"},
    {"code": "399", "name": "Muktsar"},
    {"code": "453", "name": "Pathankot"},
    {"code": "454", "name": "Patiala"},
    {"code": "501", "name": "Rupnagar (Ropar)"},
    {"code": "507", "name": "Sahibzada Ajit Singh Nagar (Mohali)"},
    {"code": "514", "name": "Sangrur"},
    {"code": "527", "name": "Shahid Bhagat Singh Nagar (Nawanshahr)"},
    {"code": "574", "name": "Tarn Taran"}
]

# dist_df = pd.DataFrame(districts)


def encode_url(url):
    sentence = url
    sentence_bytes = sentence.encode("ascii")
    base64_bytes = base64.b64encode(sentence_bytes) 
    base64_string = base64_bytes.decode("ascii")

    return base64_string

#function to decode the url
def decode_url(url):
    encoded_string = url
    enocode_ascii = encoded_string.encode("ascii")
    decoded_b64 = base64.b64decode(enocode_ascii)
    original_string = decoded_b64.decode("ascii")
    return original_string


def extract_captcha(soup):
   
    #get captcha
    captcha_tag = soup.find('img',class_="capcha-badge")

    src = captcha_tag['src']
    new_url = "https://rera.punjab.gov.in/"

    img_url = urljoin(new_url,src)

    img_response = session.get(img_url)

    if img_response.status_code == 200:
    
        img = Image.open(BytesIO(img_response.content))

        img.save(r"C:\Users\pc\Shraddha\new_env\PunjabRera\captcha_image.png")
        print("CAPTCHA image downloaded successfully.")
    else:
        print("Failed to retrieve the CAPTCHA image.")



    img = cv2.imread(r'C:\Users\pc\Shraddha\new_env\PunjabRera\captcha_image.png', cv2.IMREAD_GRAYSCALE)

    # Process image (e.g. thresholding)
    _, thresh_img = cv2.threshold(img, 150, 255, cv2.THRESH_BINARY_INV)

    # Convert to PIL Image
    pil_img = Image.fromarray(thresh_img)

    # OCR config
    custom_config = r'--oem 3 --psm 7 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'

    # Extract text
    captcha_text = pytesseract.image_to_string(pil_img, config=custom_config)

    captcha_text = captcha_text.strip().replace(" ", "").replace("\n", "")
    print(captcha_text)

    return captcha_text


def get_token(soup):
    token_input = soup.find('input', {'name': '__RequestVerificationToken'})

    # Get the value attribute (the token)
    token_value = token_input['value'] if token_input else None

    print('Token:', token_value)
    return token_value



def extract_data(soup2):
    table_rows = soup2.find_all('tr', class_='odd gradeX')

    df = []
    for tr in table_rows:

        data = {}

        try:
        # District Name
            data['district'] = tr.find_all('td')[1].text.strip()
        except Exception as e:
            logging.error(f" district name Error")

        try:
        # Project Name
            project_td = tr.find('td', class_='project-name')
            data['project_name'] = project_td['data-project-name'].strip()
        except Exception as e:
            logging.error(f"project name Error")

        # Hidden inputs
        try:
            data['project_id'] = project_td.find('input', {'name': 'item.Project_ID'})['value']
            data['promoter_id'] = project_td.find('input', {'name': 'item.Promoter_ID'})['value']
            data['promoter_type'] = project_td.find('input', {'name': 'item.PromoterType'})['value']
        except Exception as e:
            logging.error("project id Error ")

        try:    
            # Promoter Name
            data['promoter_name'] = tr.find_all('td')[3].text.strip()
        except Exception as e:
            logging.error("promoter name Error")

        # Project Diary Number
        try:
            diary_td = tr.find('td', class_='project-diary-number')
            data['project_diary_no'] = diary_td['data-diary-no'].strip()
            # data['project_diary_text'] = diary_td.text.strip()
        except Exception as e:
            logging.error(" project diary no Error")

        # Date
        try:
            data['valid_till_date'] = tr.find_all('td')[5].text.strip()
        except Exception as e:
            logging.error(f"valid_till_date Error ")

        # Links
        try:
            links_td = tr.find_all('td')[6]
            # view_details_link = links_td.find('a', {'id': 'modalOpenerButtonRegdProject'})['href']
            certificate_link = links_td.find_all('a')[1]['href']
        except Exception as e:
            logging.error(f"certificate_link Error")

        # data['view_details_link'] = view_details_link
        try:
            data['certificate_pdf_link'] = certificate_link
            if certificate_link:
                #encode pdf url
                pdf = encode_url(certificate_link)

            data['pdf'] = pdf
        except Exception as e:
            logging.error("pdf Error")

        
        now = datetime.now()
        short_month = now.strftime("%b")
        year = now.year

        data['Month'] = short_month
        data['year'] = year

        df.append(data)
    return df


for row in districts:


    district = row['name']
    code = row['code']

    max_retries = 3
    for attempt in range(1, max_retries + 1):

        logging.info(f"{district}***{code}")

        print("******", district,code,"*******")
        response = session.get(main_url)

        time.sleep(1) 

        soup = BeautifulSoup(response.content,'html.parser')

        #get token and captcha
        captcha_text = extract_captcha(soup)
        token_value = get_token(soup)

        if not captcha_text :
            logging.info(f"Attempt {attempt} for {district}...")
            continue

        post_url = "https://rera.punjab.gov.in/reraindex/PublicView/ProjectPVregdprojectInfo"


        headers = {
            "accept": "*/*",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://rera.punjab.gov.in",
            "referer": "https://rera.punjab.gov.in/reraindex/publicview/projectinfo",
            "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest"
        }

        session.headers.update(headers)

        data = {
            "__RequestVerificationToken": token_value,
            "Input_SearchOptionTabFlag": "1",
            "Input_AdvSearch_MoreOptionsFlag": "1",
            "Input_RegdProject_DistrictName": code,
            "Input_QUP_DistrictName": "",
            "Input_GeoTagging_DistrictName": "",
            "Input_AdvSearch_DistrictName": "",
            "Input_AdvSearch_SubDivisionName": "",
            "Input_RegdProject_DistrictCode": code,
            "Input_RegdProject_ProjectName": "",
            "Input_RegdProject_PromoterName": "",
            "Input_RegdProject_RERAnumberRegistration": "",
            "Input_RegdProject_CaptchaText": captcha_text,  # Your CAPTCHA text here
            "Input_QUP_DistrictCode": "",
            "Input_QUP_ProjectName": "",
            "Input_QUP_UpdatesYear": "",
            "Input_QUP_RERAnumberRegistration": "",
            "Input_QUP_CaptchaText": "",
            "Input_GeoTagging_DistrictCode": "",
            "Input_GeoTagging_TypeOfProject": "AL",
            "Input_GeoTagging_CaptchaText": "",
            "Input_AdvSearch_OptionTypeName": "REG",
            "Input_AdvSearch_DistrictCode": "",
            "Input_AdvSearch_SubDivisionCode": "",
            "Input_AdvSearch_ProjectCostFlag": "",
            "Input_AdvSearch_ProjectAreaFlag": "",
            "Input_AdvSearch_TypeOfProject": "AL",
            "parkingtypeMaster[0].ParkingType_Value": "false",
            "parkingtypeMaster[1].ParkingType_Value": "false",
            "parkingtypeMaster[2].ParkingType_Value": "false",
            "parkingtypeMaster[3].ParkingType_Value": "false",
            "parkingtypeMaster[4].ParkingType_Value": "false",
            "internalMaster[0].InternalFacilities_Value": "false",
            "internalMaster[1].InternalFacilities_Value": "false",
            "internalMaster[2].InternalFacilities_Value": "false",
            "internalMaster[3].InternalFacilities_Value": "false",
            "internalMaster[4].InternalFacilities_Value": "false",
            "internalMaster[5].InternalFacilities_Value": "false",
            "internalMaster[6].InternalFacilities_Value": "false",
            "internalMaster[7].InternalFacilities_Value": "false",
            "internalMaster[8].InternalFacilities_Value": "false",
            "internalMaster[9].InternalFacilities_Value": "false",
            "internalMaster[10].InternalFacilities_Value": "false",
            "internalMaster[11].InternalFacilities_Value": "false",
            "internalMaster[12].InternalFacilities_Value": "false",
            "internalMaster[13].InternalFacilities_Value": "false",
            "internalMaster[14].InternalFacilities_Value": "false",
            "internalMaster[15].InternalFacilities_Value": "false",
            "externalMaster[0].ExternalFacilities_Value": "false",
            "externalMaster[1].ExternalFacilities_Value": "false",
            "externalMaster[2].ExternalFacilities_Value": "false",
            "externalMaster[3].ExternalFacilities_Value": "false",
            "externalMaster[4].ExternalFacilities_Value": "false",
            "externalMaster[5].ExternalFacilities_Value": "false",
            "externalMaster[6].ExternalFacilities_Value": "false",
            "externalMaster[7].ExternalFacilities_Value": "false",
            "externalMaster[8].ExternalFacilities_Value": "false",
            "externalMaster[9].ExternalFacilities_Value": "false",
            "externalMaster[10].ExternalFacilities_Value": "false",
            "externalMaster[11].ExternalFacilities_Value": "false",
            "colonytypeMaster[0].ColonyType_Value": "false",
            "colonytypeMaster[1].ColonyType_Value": "false",
            "colonytypeMaster[2].ColonyType_Value": "false",
            "Input_MoreOptions_MegaProjectCategory": "NA",
            "Input_AdvSearch_CaptchaText": "",
            "dataTableSearchProject_length": "10",
            "dataTableSearchQUpdatesProject_length": "10"
        }

        response2 = session.post(post_url,data=data,headers=headers)
        if "Invalid Capcha Text" in response2.text:
            print("❌ CAPTCHA was incorrect")
            logging.error("❌ Incorrect Captcha")
            continue
        else:
            print("✅ CAPTCHA likely accepted — check the results table")
            logging.info("✅ CAPTCHA likely accepted ")

        time.sleep(1)
        soup2 = BeautifulSoup(response2.content,'html.parser')

        
        with open(rf'{path}\{district}.html', 'wb') as f:
            f.write(response2.content)
            logging.info("Html Saved")

        data = extract_data(soup2)
        df = pd.DataFrame(data)
        print("length of df : ",len(df))
        logging.info(f"length of df : {len(df)} for {district}")

        if not df.empty:
           
            try:
                with engine_self.begin() as connection:
                   
                    df.to_sql('tbl_main_page', if_exists='append', con=connection)
                    print("record saved")
                    logging.info("record Saved...")
                    break
            except Exception as e:
                print(f"database error {e}")
        else:
            print("No data returned,")
            logging.info(f"No data Returned for {district}")