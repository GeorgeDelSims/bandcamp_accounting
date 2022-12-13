import pandas as pd
import numpy as np
import os

def bandcamp_csv_to_dataframe(ENVPATH):
    '''
    Function that uploads the csv to a dataframe, cleans the data and gets rid of unnecessary columns.

    Args: ENVPATH (string format): relative path to the csv file

    Returns: pandas dataframe

    '''
    #Read csv and turn into dataframe
    datapath = os.environ.get(ENVPATH)
    df = pd.read_csv(datapath, encoding = "utf-8", skiprows=[0])

    #Change date column to datetime format
    df.date = pd.to_datetime(arg=df.date, errors='raise', format="%m/%d/%y %I:%M%p").dt.date
    # df.date = df.date.dt.strftime('%Y-%m-%d')

    #clean column data
    df.columns = df.columns.str.replace(' ', '_').str.replace('/', '_')

    #get rid of unnecessary columns
    df = df.drop(columns=['paid_to','ship_from_country_name', 'package', 'option', \
    'item_url','catalog_number', 'upc', 'isrc', 'buyer_name', 'buyer_email', \
        'buyer_phone', 'buyer_note', 'ship_to_name', 'ship_to_street', 'ship_to_street_2', \
            'ship_to_city', 'ship_to_state', 'seller_tax', 'ship_to_zip', 'ship_to_country', \
                'ship_to_country_code', 'buyer_country_code', 'buyer_country_name', 'ship_date', \
                    'discount_code', 'ship_notes'],\
                    axis=0)

    #Replace NaNs with 0s
    df = df.fillna(0)

    return df


def paypal_csv_to_dataframe(ENVPATH):
    '''
    Function that uploads the csv to a dataframe, cleans the data and gets rid of unnecessary columns.

    Args: ENVPATH (string format): relative path to the csv file

    Returns: pandas dataframe

    '''
    #Read csv and turn into dataframe
    paypal_url = os.environ.get(ENVPATH)
    paypal_df = pd.read_csv(paypal_url)

    #Turn date into datetime formats
    paypal_df.Date = pd.to_datetime(arg=paypal_df.Date, errors='raise', format="%d/%m/%Y")

    #Clean column data
    paypal_df.columns = paypal_df.columns\
        .str.replace(' ', '_')\
        .str.replace('/', '_')
    paypal_df.columns = [column.lower() for column in paypal_df.columns]

    #Remove unnecessary columns
    paypal_df = paypal_df.drop(['time', 'timezone', 'from_email_address','to_email_address','shipping_address','item_id','insurance_amount',\
        'option_1_name','option_1_value','option_2_name','option_2_value','reference_txn_id','invoice_number','custom_number',\
            'receipt_id','address_line_1','address_line_2_district_neighborhood','town_city','state_province_region_county_territory_prefecture_republic',\
                'zip_postal_code','country','contact_phone_number','note','country_code'], axis=1)

    #Seperate debit and credit columns
    paypal_debit = paypal_df[paypal_df['balance_impact'] == 'Debit']
    paypal_credit = paypal_df[paypal_df['balance_impact'] == 'Credit']

    #Replace NaNs with 0s
    paypal_df = paypal_df.fillna(0)

    return paypal_df
