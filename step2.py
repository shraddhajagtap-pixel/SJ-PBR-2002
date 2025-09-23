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
from lxml import html 
import re
from pprint import pprint
from sqlalchemy import create_engine, text
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from urllib.parse import quote

pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

engine_115 = create_engine("mysql://Shradha:%s@192.168.0.115:3306/punjab_rera" % quote("Efcdata@2025"))

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
})


##set current month and year
now = datetime.now()
month = now.strftime("%b")
year = now.year

def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip()

#function to encode the url

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


def insert_data(df, table):
    flag = False
    try:
        with engine_115.begin() as connection:
            df.to_sql(table, con = connection, if_exists='append', index=False)
            flag = True
    except Exception as e:
        print(e)
    return flag

## 1) promoter Details  key value
def get_promoter_data(promoter_table,rera_id):
    # Iterate over all rows
    data = {}

    for tr in promoter_table.find_all('tr'):
        tds = tr.find_all('td')

        # If there are 4 tds, we have two key-value pairs
        if len(tds) == 4:
            # First key-value
            key1 = clean_text(tds[0].get_text(strip=True))
            val1 = clean_text(tds[1].get_text(separator=' ', strip=True))

            # Second key-value
            key2 = clean_text(tds[2].get_text(strip=True))
            val2 = clean_text(tds[3].get_text(separator=' ', strip=True))

            data[key1] = val1
            data[key2] = val2

        # If there are 2 tds, the second spans 3 columns and represents one key-value pair
        elif len(tds) == 2:
            key = clean_text(tds[0].get_text(strip=True))
            val = clean_text(tds[1].get_text(separator=' ', strip=True))
            data[key] = val

    promoter_df = pd.DataFrame([data])
    promoter_df['rera_id'] = rera_id

    now = datetime.now()
    month = now.strftime("%b")
    year = now.year

    promoter_df['month'] = month
    promoter_df['year'] = year

    pd.set_option('display.max_colwidth', None)
    promoter_df = promoter_df.rename(columns={
    'Years of Experience of Promoter in Real Estate Development in Punjab': 'Years_Experience_RE_Punjab','Years of Experience of Promoter in Real Estate Development in Other states or UTs': 'Years_Experience_RE_In_Other_States'
    })
    return promoter_df

# 2) get member details

def get_member_data(member_table,rera_id):  
    
    members = []


    # Process each row in the table body
    for tr in member_table.select('table#dataTable tbody tr'):
        tds = tr.find_all('td')
        if len(tds) < 5:
            continue  # Not a valid member row

        designation = clean_text(tds[1].get_text())
        name = clean_text(tds[2].get_text())

        # Address and email are inside a <span>
        address_block = tds[3].get_text(separator=' ', strip=True)
        address_block = re.sub(r'\s+', ' ', address_block)

        # Extract email from text
        email_match = re.search(r'Email:\s*(\S+)', address_block)
        email = email_match.group(1).replace('[at]', '@') if email_match else ''

        # Remove 'Email:' part from address
        address = re.sub(r'Email:\s*\S+', '', address_block).strip()

        # Get photo src
        img_tag = tds[4].find('img')
        photo_url = img_tag['src'].replace('\\', '/') if img_tag and 'src' in img_tag.attrs else ''

        encoded_url = encode_url(photo_url) if photo_url else None

        members.append({
            "Designation": designation,
            "Name": name,
            "Address": address,
            "Email": email,
            "Photo URL": photo_url,
            "rera_id" : rera_id,
            "encoded_url" : encoded_url
        })

    # Display the results
    now = datetime.now()
    month = now.strftime("%b")
    year = now.year

    member_df = pd.DataFrame(members)
    member_df['month'] = month
    member_df['year'] = year

    member_df.columns = [col[:60] for col in member_df.columns]     
    return member_df

## 4) project data
def get_project_data(about_table,rera_id):

    # Assuming `project_table` is a BeautifulSoup object of the table
    data = {}


    # Iterate over all rows in the project table
    for tr in about_table.find_all('tr'):

        tds = tr.find_all('td')

        if len(tds) == 4:
            key1 = clean_text(tds[0].get_text())
            val1 = clean_text(tds[1].get_text(separator=' ', strip=True))
            key2 = clean_text(tds[2].get_text())
            val2 = clean_text(tds[3].get_text(separator=' ', strip=True))

            val1 = val1.replace('[at]', '@')
            val2 = val2.replace('[at]', '@')

            data[key1] = val1
            data[key2] = val2

        elif len(tds) == 2:
            key = clean_text(tds[0].get_text())

            val = clean_text(tds[1].get_text(separator=' ', strip=True))
            val = val.replace('[at]', '@')
            data[key] = val

            if key == "Authorized Person for Communication with regards to Project":
                try:
                    span = tds[1].find('span')

                    for child in span.children:
                        if child.name == 'br':
                            break
                        if isinstance(child, str):
                            name = child.strip()
                            if name:
                            
                                name = name.replace('\n','')
                                name = name.replace('\r','')
                                name.strip()
                                print("Name:", name)
                                data['Authorized Person Name'] = name  
                                break
                except Exception as e:
                    print(e)


            auth_key = "Authorized Person for Communication with regards to Project"
            if auth_key in data:
                raw_text = data[auth_key]
                raw_text = re.sub(r'\s+', ' ', raw_text)

                email_match = re.search(r'E[-\s]?Mail:\s*([^\s]+)', raw_text)
                phone_match = re.search(r'Mobile Phone:\s*(\d+)', raw_text)

                email = email_match.group(1).replace('[at]', '@') if email_match else ''
                phone = phone_match.group(1) if phone_match else ''

                address = re.sub(r'E[-\s]?Mail:.*', '', raw_text).strip()


                    # Fallback: take the first few words before first comma or 'br'
                

                # Hardcoded from HTML
                data['Authorized Person Address'] = address
                data['Authorized Person Email'] = email
                data['Authorized Person Phone'] = phone



    # -- Final Step: Create DataFrame --
    project_df = pd.DataFrame([data])
    project_df['rera_id'] = rera_id

    now = datetime.now()
    month = now.strftime("%b")
    year = now.year

    project_df['month'] = month
    project_df['year'] = year

    # For better display (optional)
    pd.set_option('display.max_colwidth', None)
    project_df = project_df.rename(columns={
    'Proposed/ Expected Date of Project Completion as specified in Form B': 'expected_project_completion_date',
    'Specification Details of Proposed Project as per the Brochure/ Prospectus':'specification',
    'Authorized Person for Communication with regards to Project' : 'Authorized_person_details'
    })
    # Display DataFrame
    return project_df

## 5) land details
def get_land_area_details(table,rera_id):
    data = {}

    def clean_text(text):
        return re.sub(r'\s+', ' ', text).strip().replace('(in sqr mtrs)', '').strip()

    for tr in table.find_all('tr'):
        tds = tr.find_all('td')
        
        i = 0
        while i < len(tds):
            label_elem = tds[i].find('label')
            value_elem = tds[i + 1] if i + 1 < len(tds) else None

            if label_elem and value_elem:
                key = clean_text(label_elem.get_text())
                val = clean_text(value_elem.get_text())
                data[key] = val
            i += 2

    land_df = pd.DataFrame([data])
    land_df['rera_id'] = rera_id

    now = datetime.now()
    month = now.strftime("%b")
    year = now.year

    land_df['month'] = month
    land_df['year'] = year

    land_df = land_df.rename(columns = {'Total Area of Land Proposed to be developed':'total_area',
                                    'Area under group housing development excluding common areas and ameneties':'area_under_housing_development',
                                    'Area under residential plotted development excluding common areas and ameneties':'area_under_residential_development',
                                    'Area under commercial development excluding common areas and ameneties':'area_under_commercial_development',
                                    'Area under industrial development excluding common areas and ameneties':'area_under_industrial_development',
                                    'Area under common amenties servicing the entire project':'area_under_servicing'
                                    })
    return land_df


##7) project plans documents
def extract_project_plans(table,rera_id):
    rows = table.find('tbody').find_all('tr')
    document_data = []

    for tr in rows:
        tds = tr.find_all('td')
        if len(tds) == 5:
            sr_no = tds[0].get_text(strip=True)
            doc_name = tds[1].get_text(strip=True)
            ref_no = tds[2].get_text(strip=True).replace('--', '').strip()
            issue_date = tds[3].get_text(strip=True).replace('--', '').strip()
            link_tag = tds[4].find('a')

            # Get PDF link and fix backslashes
            encoded_pdf = ''
            pdf_url = link_tag['href'].replace('\\', '/') if link_tag else None
            if pdf_url and not pdf_url.startswith("http"):
                pdf_url = "https://rera.punjab.gov.in" + pdf_url
                
            encoded_pdf = encode_url(pdf_url) if pdf_url else None

            

            document_data.append({
                'SrNo': sr_no,
                'Document Name': doc_name,
                'Reference Number': ref_no or None,
                'Issue Date': issue_date or None,
                'PDF Link': pdf_url,
                'encoded_pdf' :encoded_pdf
            })

    project_plan_df = pd.DataFrame(document_data)

    project_plan_df['rera_id'] = rera_id
    
    now = datetime.now()
    month = now.strftime("%b")
    year = now.year

    project_plan_df['month'] = month
    project_plan_df['year'] = year

    project_plan_df.columns = [col[:60] for col in project_plan_df.columns]
    return project_plan_df

## 8)building details
def extract_inventory_details(rows,rera_id):
    now = datetime.now()
    month = now.strftime("%b")
    year = now.year
    all_df = pd.DataFrame()



    for tr in rows:
        tds = tr.find_all('td')
        if len(tds) == 3:
            sr_no = clean_text(tds[0].get_text(strip=True))
            building = clean_text(tds[1].get_text(strip=True))
            floor_plot = clean_text(tds[2].get_text())
            continue


        target_table = tr.find('table',id='dataTable')

        if target_table:
            table_html = str(target_table)
            inner_df = pd.read_html(table_html)[0]

            inner_df['srno'] = sr_no
            inner_df['building_name'] = building
            inner_df['floor_plot'] = floor_plot
            inner_df['rera_id'] = rera_id
            inner_df['month'] = month
            inner_df['year'] = year

            if inner_df.empty:
                # Create one row with outer info and NaNs for inner columns
                columns = inner_df.columns.tolist()
                # Create a dict with NaN for inner columns except the outer info columns
                row = {col: (None if col not in ['srno', 'building_name', 'floor_plot', 'rera_id', 'month', 'year'] else inner_df[col].iloc[0] if not inner_df.empty else None) for col in columns}
                # Update outer info explicitly (in case inner_df is empty)
                row.update({
                    'srno': sr_no,
                    'building_name': building,
                    'floor_plot': floor_plot,
                    'rera_id': rera_id,
                    'month': month,
                    'year': year
                })
                inner_df = pd.DataFrame([row])

            all_df = pd.concat([all_df, inner_df], ignore_index=True)
    return all_df

## 12) proffessionals details
def extract_professional_table(table):
    rows = table.find('tbody').find_all('tr')

    prof = []
    
    data = {}
    print(len(rows))
    for tr in rows:
        tds = tr.find_all('td')
        # Check if it's a main row (has 5 columns)
        if len(tds) == 5:
            data['sr_no'] = tds[0].get_text(strip=True)
            data['Name of Professional'] = tds[1].get_text(strip=True)
            data['Associated Consultant Type'] = tds[2].get_text(strip=True)
            data['Name & Year of Establishment of Promoter'] = tds[3].get_text(strip=True)
            data['Name & Profile of Key Projects Completed'] = tds[4].get_text(strip=True)
            continue
            # Assume the next row is address

        if len(tds) == 3:

            data['Address'] = clean_text(tds[2].get_text(separator=' ', strip=True))
            full_address = data['Address']

            data['Address'] = full_address.replace('[at]', '@') if full_address else ''  

            auth_key = "Address"
            if auth_key in data:
                raw_text = data[auth_key]
                raw_text = re.sub(r'\s+', ' ', raw_text)
    
                email_match = re.search(r'e[-\s]?mail:\s*([^\s]+)', raw_text, re.IGNORECASE)
                phone_match = re.search(r'Mobile/Landline Number:\s*(\d+)', raw_text)
    
                email = email_match.group(1).replace('[at]', '@') if email_match else ''
                phone = phone_match.group(1) if phone_match else ''
    
                address = re.sub(r'E[-\s]?Mail:.*', '', raw_text).strip()
                data['Address'] = address
                data['Email'] = email
                data['Phone'] = phone


            prof.append(data)
            data = {}

    df = pd.DataFrame(prof)
    return df


try:
    with engine_115.begin() as connection:
        sql = "SELECT * FROM punjab_rera.tbl_main_page "
        dfs = pd.read_sql(sql, con=connection)
        df = dfs[1012:]
except Exception as e:
    print(f"database error {e}")


##iteration
for index,row in df.iterrows():
    genId = row['genId']

    print("** processing genid : **",genId)

    project_id = row['project_id']
    promoter_id = row['promoter_id']
    promoter_type = row['promoter_type']
    rera_id = row['project_diary_no']

    url = f"https://rera.punjab.gov.in/reraindex/PublicView/ProjectViewDetails?inProject_ID={project_id}&inPromoter_ID={promoter_id}&inPromoterType={promoter_type}"

    response = session.get(url)
    
    response.status_code
    soup = BeautifulSoup(response.content,'html.parser')

    ## access all tables
    tables1 = pd.read_html(str(soup))


    ## get promoter details 
    try:
        tree = html.fromstring(response.text)
        target_divs = tree.xpath("//div[contains(text(), 'Promoter Details ')]")
        parent = target_divs[0].getparent()
        promoter_table = BeautifulSoup(html.tostring(parent, pretty_print=True).decode(),'html.parser' )
        promoter_df = get_promoter_data(promoter_table,rera_id)
        flag = insert_data(promoter_df,'tbl_pb_rera_promoter_details')
        print("promoter details :",flag)
    except Exception as e:
        print('ERROR',e)
        missing_df = pd.DataFrame()
        missing_df['genId'] = genId
        flag = insert_data(missing_df,'pb_missing_genid')


    ## member details
    try:
        tree = html.fromstring(response.text)
        target_divs = tree.xpath("//div[contains(text(), 'Organization Member Details ')]")
        parent = target_divs[0].getparent()

        member_table = BeautifulSoup(html.tostring(parent, pretty_print=True).decode(),'html.parser' )
        member_df = get_member_data(member_table,rera_id)

        flag = insert_data(member_df,'tbl_pb_rera_member_details')
        print("member details :",flag)
    except Exception as e:
        print('ERROR',e)
        missing_df = pd.DataFrame()
        missing_df['genId'] = genId
        flag = insert_data(missing_df,'pb_missing_genid')


    ## letigations
    try:
        for data in tables1:
            if 'Case Title' in data.columns:
                litigation = data
                break
            
        litigation['rera_id'] = rera_id
        litigation['month'] = month
        litigation['year'] = year

        litigation = litigation.rename(columns = {'Authority/Forum Name where case is Pending/Resolved':'authority_name'})
        flag = insert_data(litigation,'tbl_pb_rera_letigation_details')
        print("litigation details :",flag)
    except Exception as e:
        print('ERROR',e)
        missing_df = pd.DataFrame()
        missing_df['genId'] = genId
        flag = insert_data(missing_df,'pb_missing_genid')


    ## project details
    try:
        tree = html.fromstring(response.text)
        target_divs = tree.xpath("//div[contains(text(), 'About the Project')]")
        parent = target_divs[0].getparent()

        about_table = BeautifulSoup(html.tostring(parent, pretty_print=True).decode(),'html.parser' )
        project_df = get_project_data(about_table,rera_id)

        flag = insert_data(project_df,'tbl_pb_rera_project_details')
        print("project details :",flag)
    except Exception as e:
        print('ERROR',e)
        missing_df = pd.DataFrame()
        missing_df['genId'] = genId
        flag = insert_data(missing_df,'pb_missing_genid')


    ## land details
    try:
        tree = html.fromstring(response.text)
        target_divs = tree.xpath("//div[contains(text(), 'Project Land Details')]")
        parent = target_divs[0].getparent()

        land_table = BeautifulSoup(html.tostring(parent, pretty_print=True).decode(),'html.parser' )
        land_df = get_land_area_details(land_table,rera_id)

        flag = insert_data(land_df,'tbl_pb_rera_land_details')
        print("land details :",flag)
    except Exception as e:
        print('ERROR',e)
        missing_df = pd.DataFrame()
        missing_df['genId'] = genId
        flag = insert_data(missing_df,'pb_missing_genid')


    ## Khasra details
    try:
        for data in tables1:
            if 'Khasra Number of Land proposed to be developed' in data.columns:
                khasra = data
                break
            
        khasra['rera_id'] = rera_id
        now = datetime.now()
        month = now.strftime("%b")
        year = now.year
        khasra['month'] = month
        khasra['year'] = year

        flag = insert_data(khasra,'tbl_pb_rera_khasra_details')
        print("khasra details :",flag)  
    except Exception as e:
        print('ERROR',e)
        missing_df = pd.DataFrame()
        missing_df['genId'] = genId
        flag = insert_data(missing_df,'pb_missing_genid')


        ##  project plan documents
    try:
        tree = html.fromstring(response.text)
        target_divs = tree.xpath("//div[contains(text(), 'Project Plan(s)')]")
        parent = target_divs[0].getparent()

        project_plan_table = BeautifulSoup(html.tostring(parent, pretty_print=True).decode(),'html.parser' )
        project_plan_df = extract_project_plans(project_plan_table,rera_id)

        flag = insert_data(project_plan_df,'tbl_pb_rera_project_plan_docs')
        print("project_plan  details :",flag)
    except Exception as e:
        print('ERROR',e)
        missing_df = pd.DataFrame()
        missing_df['genId'] = genId
        flag = insert_data(missing_df,'pb_missing_genid')


    ## building details
    try:
        all_df = pd.DataFrame()
        tree = html.fromstring(response.text)
        target_divs = tree.xpath("//div[contains(text(), 'Project Building/ Tower/ Block Construction & Inventory Details')]")
        parent = target_divs[0].getparent()
        building_table = BeautifulSoup(html.tostring(parent, pretty_print=True).decode(),'html.parser' )
        rows = building_table.find('tbody').find_all('tr')

        inventory_df = extract_inventory_details(rows,rera_id)

        flag = insert_data(inventory_df,'tbl_pb_rera_building_details')
        print("building details :",flag)  
    except Exception as e:
        print('ERROR',e)
        missing_df = pd.DataFrame()
        missing_df['genId'] = genId
        flag = insert_data(missing_df,'pb_missing_genid')


    ## parking
    try:
        for data in tables1:
            if 'Type of Parking' in data.columns:
                parking_df = data
                break
            
        parking_df['rera_id'] = rera_id
        month = now.strftime('%b')
        parking_df['month'] = month
        parking_df['year'] = year

        flag = insert_data(parking_df,'tbl_pb_rera_parking_details')
        print("parking details :",flag)  
    except Exception as e:
        print('ERROR',e)
        missing_df = pd.DataFrame()
        missing_df['genId'] = genId
        flag = insert_data(missing_df,'pb_missing_genid')


    ## Amenities
    try:
        for data in tables1:
            if 'Internal Infrastructure Facilities Name' in data.columns:
                internal_facility_df = data
                break

        internal_facility_df['rera_id']= rera_id
        internal_facility_df['month'] = month
        internal_facility_df['year'] = year

        flag = insert_data(internal_facility_df,'tbl_pb_rera_internal_facility_details')
        print("internal_facility details :",flag)  
    except Exception as e:
        print('ERROR',e)
        missing_df = pd.DataFrame()
        missing_df['genId'] = genId
        flag = insert_data(missing_df,'pb_missing_genid')

    ##
    try:
        for data in tables1:
            if 'External Infrastructure Facilities Name' in data.columns:
                External_facility_df = data
                break
        External_facility_df['rera_id']= rera_id
        External_facility_df['month'] = month
        External_facility_df['year'] = year

        flag = insert_data(External_facility_df,'tbl_pb_rera_External_facility_details')
        print("External_facility details :",flag)  
    except Exception as e:
        print('ERROR',e)
        missing_df = pd.DataFrame()
        missing_df['genId'] = genId
        flag = insert_data(missing_df,'pb_missing_genid')

    ##professional details
    try:
        tree = html.fromstring(response.text)
        target_divs = tree.xpath("//div[contains(text(), 'Project Professionals')]")
        parent = target_divs[0].getparent()

        project_professionals_table = BeautifulSoup(html.tostring(parent, pretty_print=True).decode(),'html.parser' )
        project_professionals_df = extract_professional_table(project_professionals_table)
        project_professionals_df
        project_professionals_df['rera_id']= rera_id
        project_professionals_df['month'] = month
        project_professionals_df['year'] = year

        flag = insert_data(project_professionals_df,'tbl_pb_rera_professionals_details')
        print("project_professionals details :",flag)  
    except Exception as e:
        print('ERROR',e)
        missing_df = pd.DataFrame()
        missing_df['genId'] = genId
        flag = insert_data(missing_df,'pb_missing_genid')







    
