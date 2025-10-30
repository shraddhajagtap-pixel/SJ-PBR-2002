import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
from urllib.parse import quote
from datetime import datetime

engine_115 = create_engine("mysql://Shradha:%s@192.168.0.115:3306/punjab_rera" % quote("Efcdata@2025"))
engine_116 = create_engine("mysql://Shraddha:%s@192.168.0.116:3306/finalReraData" % quote("Efcdata@2025"))

##set current month and year

now = datetime.now()
month = now.strftime("%b")
year = now.year

def insert_data(df, table):
    flag = False
    try:
        with engine_116.begin() as connection:
            df.to_sql(table, con = connection, if_exists='append', index=False)
            flag = True
    except Exception as e:
        print(f"Error while inserting data {e}")
    return flag

def clean_string(s):
    if pd.isna(s):
        return ''
    return re.sub(r'\s+', ' ', str(s)).strip()

def capitalize_words(s):
    if pd.isna(s):
        return ''
    return ' '.join([word.capitalize() for word in str(s).split()])


## main page df 
try:
    with engine_115.begin() as connection:
        sql = "SELECT * FROM punjab_rera.tbl_main_page where Month = %s "
        main_df = pd.read_sql(sql, con=connection,params=(month,))
except Exception as e:
    print(f"database error {e}")

main_df['projectName'] = main_df['project_name'].apply(lambda x: clean_string(capitalize_words(x)))
main_df['DeveloperName'] = main_df['promoter_name'].apply(lambda x: clean_string(capitalize_words(str(x).split('(')[0])))
main_df['state'] = 'Punjab'

## project df

try:
    with engine_115.begin() as connection:
        sql = "SELECT * FROM punjab_rera.tbl_pb_rera_project_details where Month = %s "
        project_df = pd.read_sql(sql, con=connection,params=(month,))
except Exception as e:
    print(f"database error {e}")

project_df['projectAddress'] = project_df['Project Address'].apply(lambda x: capitalize_words(x))

def project_type(ptype):
    ptype = str(ptype).lower()
    if 'commercial' in ptype and 'residential' not in ptype:
        return 'Commercial'
    if 'residential' in ptype and 'commercial' not in ptype and 'industrial' not in ptype:
        return 'Residential'
    if 'industrial' in ptype and 'residential' not in ptype:
        return 'Commercial'
    if 'commercial' in ptype and 'residential' in ptype:
        return 'Mixed'
    if 'commercial' in ptype and 'industrial' in ptype and 'residential' not in ptype:
        return 'Commercial'
    if 'residential' in ptype and 'industrial' in ptype and 'commercial' not in ptype:
        return 'Mixed'
    if 'commercial' in ptype and 'residential' in ptype and 'industrial' in ptype:
        return 'Mixed'
    if 'industrial' in ptype and 'residential' in ptype and 'commercial' not in ptype:
        return 'Mixed'
    return 'Others'

# Replace '--' with 0 and remove '(INR)'
def clean_project_cost(x):
    if pd.isna(x):  # handle missing values
        return 0
    x = str(x)
    if '--' in x:
        return 0
    x = x.replace('(INR)', '').replace(',', '').strip()  # remove commas too
    try:
        return round(float(x))
    except:
        return 0
    

project_df['projectType'] = project_df['Type of Project'].apply(project_type)
project_df['projectStatus'] =  project_df['Project Status']
project_df['authorizedPersonContact'] = project_df['Authorized Person Phone']
project_df['webLink'] = project_df['Project Web Link']
project_df['projectStartDate'] = pd.to_datetime(project_df['Project Start Date'], format='%d-%b-%Y', errors='coerce')
project_df['projectDateOfCompletion'] = pd.to_datetime(project_df['expected_project_completion_date'], format='%d-%b-%Y', errors='coerce')

project_df['projectCost'] = project_df['Project Cost (in rupees)'].apply(clean_project_cost)

project_df['authorizedPerson'] = project_df['Authorized Person Name'].apply(capitalize_words)
project_df['authorizedPersonAddress'] = project_df['Authorized Person Address'].apply(capitalize_words)
project_df['authorizedPersonEmail'] = project_df['Authorized Person Email'].str.lower()

project_df['projectLitigation'] = project_df['Litigation(s) related to Project'].apply(lambda x: 'Yes' if str(x).lower()=='yes' else 'No')
project_df['pincode'] = project_df['Project Address'].str.extract(r'(\d{6})')

## building details

try:
    with engine_115.begin() as connection:
        sql = "SELECT * FROM punjab_rera.tbl_pb_rera_building_details where Month = %s "
        building_df = pd.read_sql(sql, con=connection,params=(month,))
except Exception as e:
    print(f"database error {e}")


building_agg = building_df.groupby('rera_id').agg(
    totalFlats=('Total Number of Apartment/ Shop/ Plot', lambda x: int(sum(pd.to_numeric(x, errors='coerce').fillna(0)))),
    soldFlats=('Number of Apartment/ Shop/ Plot already sold', lambda x: int(sum(pd.to_numeric(x, errors='coerce').fillna(0)))),
    areaOfFlats=('Carpet Area of Apartment/ Shop/ Plot', 
                 lambda x: int(sum([round(float(str(i).split('(')[0]) * 10.7639) 
                                    for i in x if pd.notna(i)]))),
    areaOfOpenTerrace=('Exclusive OpenTerrace Area', 
                       lambda x: int(sum([round(float(str(i).split('(')[0]) * 10.7639) 
                                          for i in x if pd.notna(i)])))
).reset_index()



## land details

try:
    with engine_115.begin() as connection:
        sql = "SELECT * FROM punjab_rera.tbl_pb_rera_land_details where Month = %s "
        land_df = pd.read_sql(sql, con=connection,params=(month,))
except Exception as e:
    print(f"database error {e}")

# Convert to numeric safely and apply sqft conversion (10.7639) + rounding
land_df['totalArea'] = (pd.to_numeric(land_df['total_area'], errors='coerce').fillna(0) * 10.7639).round().astype(int)
land_df['promoterArea'] = (pd.to_numeric(land_df['Area of Land Owned by Promoter'], errors='coerce').fillna(0) * 10.7639).round().astype(int)
land_df['areaNotOwnedByPromoter'] = (pd.to_numeric(land_df['Area of Land Not Owned by Promoter'], errors='coerce').fillna(0) * 10.7639).round().astype(int)
land_df['housingDevelopmentArea'] = (pd.to_numeric(land_df['area_under_housing_development'], errors='coerce').fillna(0) * 10.7639).round().astype(int)
land_df['residentialDevelopmentArea'] = (pd.to_numeric(land_df['area_under_residential_development'], errors='coerce').fillna(0) * 10.7639).round().astype(int)
land_df['commercialDevelopmentArea'] = (pd.to_numeric(land_df['area_under_commercial_development'], errors='coerce').fillna(0) * 10.7639).round().astype(int)
land_df['industrialDevelopmentArea'] = (pd.to_numeric(land_df['area_under_industrial_development'], errors='coerce').fillna(0) * 10.7639).round().astype(int)
land_df['underServicingArea'] = (pd.to_numeric(land_df['area_under_servicing'], errors='coerce').fillna(0) * 10.7639).round().astype(int)



## parking details

try:
    with engine_115.begin() as connection:
        sql = "SELECT * FROM punjab_rera.tbl_pb_rera_parking_details where Month = %s "
        parking_df = pd.read_sql(sql, con=connection,params=(month,))
except Exception as e:
    print(f"database error {e}")

# Create ParkingType column
parking_df['ParkingType'] = parking_df['Type of Parking'].apply(lambda x: 'Open' if str(x).lower() == 'open' else 'Closed')

# Aggregate by rera_id and ParkingType
parking_agg = parking_df.groupby(['rera_id', 'ParkingType']).agg(
    parkingArea=('Total Area of Parking Space', lambda x: int(sum([round(float(str(i).split('(')[0]) * 10.7639) for i in x if pd.notna(i)]))),
    numberOfParking=('Total Number of Parking Space', 'sum'),
    numberOfSoldParking=('No of Parking Space Booked or Sold', 'sum')
).unstack(fill_value=0)

# Rename columns to match desired names
parking_agg.columns = [
    'openParkingArea' if 'parkingArea' in col and 'Open' in col else
    'numberOfOpenParking' if 'numberOfParking' in col and 'Open' in col else
    'numberOfOpenSoldParking' if 'numberOfSoldParking' in col and 'Open' in col else
    'closedParkingArea' if 'parkingArea' in col and 'Closed' in col else
    'numberOfClosedParking' if 'numberOfParking' in col and 'Closed' in col else
    'numberOfClosedSoldParking' if 'numberOfSoldParking' in col and 'Closed' in col else col
    for col in parking_agg.columns
]

parking_agg = parking_agg.reset_index()



### promoter details

try:
    with engine_115.begin() as connection:
        sql = "SELECT * FROM punjab_rera.tbl_pb_rera_promoter_details where Month = %s "
        promoter_df = pd.read_sql(sql, con=connection,params=(month,))
except Exception as e:
    print(f"database error {e}")

promoter_df['promoterAddress'] = promoter_df['Official Address']
promoter_df['promoterDistrictName'] = promoter_df['promoterDistrict']
promoter_df['promoterStateName'] = promoter_df['promoterState']
promoter_df['promoterMobileNo'] = promoter_df['Phone Number'] 
promoter_df['promoterEmail'] = promoter_df['Email Address']




## Final Merging

# Step 1: Merge all tables on project_diary_no / rera_id
final_df = main_df.merge(
    project_df, left_on='project_diary_no', right_on='rera_id', how='left', suffixes=('', '_proj')
).merge(
    building_agg, left_on='project_diary_no', right_on='rera_id', how='left', suffixes=('', '_bld')
).merge(
    land_df, left_on='project_diary_no', right_on='rera_id', how='left', suffixes=('', '_land')
).merge(
    parking_agg, left_on='project_diary_no', right_on='rera_id', how='left', suffixes=('', '_park')
)

# Step 2: Rename project_diary_no to projectReraId
final_df = final_df.rename(columns={'project_diary_no': 'projectReraId'})

# Step 3: Fill missing necessary columns
necessary_cols_fixed = [
    'projectReraId',
    'projectName',
    'DeveloperName',
    'projectAddress',
    'district',
    'state',
    'projectType',
    'projectStatus',
    'projectStartDate',
    'projectDateOfCompletion',
    'projectCost',
    'authorizedPerson',
    'authorizedPersonAddress',
    'authorizedPersonEmail',
    'authorizedPersonContact',
    'webLink',
    'projectLitigation',
    'pincode',
    'totalFlats',
    'soldFlats',
    'areaOfFlats',
    'areaOfOpenTerrace',
    'totalArea',
    'promoterArea',
    'areaNotOwnedByPromoter',
    'housingDevelopmentArea',
    'residentialDevelopmentArea',
    'commercialDevelopmentArea',
    'industrialDevelopmentArea',
    'underServicingArea',
    'openParkingArea',
    'numberOfOpenParking',
    'numberOfOpenSoldParking',
    'closedParkingArea',
    'numberOfClosedParking',
    'numberOfClosedSoldParking',
    'promoterAddress',
    'promoterDistrictName',
    'promoterStateName',
    'promoterPincode',
    'promoterEmail',
    'promoterMobileNo',
    'monthFy'
]

for col in necessary_cols_fixed:
    if col not in final_df.columns:
        final_df[col] = None  # Fill missing numeric/text columns with None

# Step 4: Select only necessary columns in the correct order
final_final_df = final_df[necessary_cols_fixed]

## adding month year
final_final_df['monthFy'] = main_df['month'].astype(str) + '-' + main_df['year'].astype(str)
# final_final_df is now ready with only necessary columns
flag = insert_data(final_final_df,'pb_ReraProjectFtbl')
print("Final dataFrame stored in table pb_ReraProjectFtbl",flag)

