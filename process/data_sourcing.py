import pandas as pd
import numpy as np
import os
import re

def bandcamp_csv_to_dataframe(ENVPATH):
    '''
    Function that uploads the csv to a dataframe, cleans the data and gets rid of unnecessary columns.

    Args: ENVPATH (string format): relative path to the csv file

    Returns: pandas dataframe

    '''
    #Read csv and turn into dataframe
    datapath = os.environ.get(ENVPATH)
    df = pd.read_csv(datapath, encoding = "utf-8", skiprows=[0],decimal=',')

    #Change date column to datetime format
    df.date = pd.to_datetime(arg=df.date, errors='raise', format="%m/%d/%y %I:%M%p").dt.date

    #clean column data
    df.columns = df.columns.str.replace(' ', '_')\
        .str.replace('/', '_')
    df.rename(columns={'paypal_transaction_id' : 'transaction_id'}, inplace=True)

    #Change numeric columns to floats
    numcols = df.columns.drop(['date', 'item_type', 'item_name',\
        'artist', 'currency','fee_type', 'bandcamp_transaction_id', 'transaction_id'])
    df[numcols] = df[numcols].apply(pd.to_numeric, errors='coerce')

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
    paypal_df = pd.read_csv(paypal_url ,decimal=',')

    #Turn date into datetime formats
    paypal_df.Date = pd.to_datetime(arg=paypal_df.Date, errors='raise', format="%d/%m/%Y")

    #Clean column data
    paypal_df.columns = paypal_df.columns\
        .str.replace(' ', '_')\
        .str.replace('/', '_')
    paypal_df.columns = [column.lower() for column in paypal_df.columns]

    #turn numeric columns into real numeric values
    numcols = paypal_df.columns.drop(['date', 'name', 'type', 'status',\
        'currency', 'transaction_id', 'address_status',\
            'item_title', 'subject', 'balance_impact'])
    paypal_df[numcols] = paypal_df[numcols].apply(pd.to_numeric, errors='coerce')

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


def get_conversion_chart(ENVPATH, start_date='2017-01-01'):
    '''
    This function turns the currency conversion csv into numerical and datetime values in a pandas dataframe.
    This makes it easier to use with the other data.

    Args:
        - ENVPATH : relative path to the csv file
        - start_date: the start date for the currency conversion (default = 2017-01-01)

    Returns: Pandas dataframe
    '''
    #Read csv
    datapath = os.environ.get(ENVPATH)
    conversion_chart = pd.read_csv(datapath, encoding = "utf-8")

    #remove unnecessary columns and rename remaining ones
    conversion_chart = conversion_chart.drop(columns=['conf', 'status1'], axis=1)\
    .rename(columns={'s1' : 'CHF_to_EUR'})

    #dates to datetime
    conversion_chart.date = pd.to_datetime(arg=conversion_chart.date, errors='raise', format="%Y-%m-%d")

    #sort according to start_date (no need for a massive dataframe withn unused values)
    conversion_chart = conversion_chart[conversion_chart['date'] > start_date]

    return conversion_chart


def get_Postfinance_info(ENVPATH):
    '''
    This function extracts information from a csv file downloaded from Postfinance.
    It will likely not be usable in any other situation as it is really adapted to this particular file.
    It can however be modified and parts of it re-used for new csv files from Postfinance or other banking systems.

    Args:
        - ENVPATH : (string) path to the csv file
        -
    '''
    #Find csv and turn into dataframe
    bank_statement = pd.read_csv(os.environ.get(ENVPATH), encoding = "utf-8")

    #Regex to find amounts and dates
    amount_pattern = ";(\d*\.\d*);"
    date_pattern = "\d{2}\.\d{2}\.\d{4}"

    #instantiate lists for appending
    amounts = []
    dates = []

    #iterate over df to extract values with regex
    for string in bank_statement['Datum von:;="15.12.2020"'][5:-2]:
        m = re.findall(amount_pattern, string, flags=0)
        n = re.findall(date_pattern, string, flags=0)
        amounts.append(m)
        dates.append(n)

    #get rid of double date values
    new_dates = []
    for x in range(len(dates)):
        new_dates.append(dates[x][0])

    #turn amounts into floats
    n_amounts = []
    for x in range(len(amounts)):
        n_amounts.append(float(amounts[x][0]))

    #create df with extracted values
    df_bank = pd.DataFrame()
    df_bank['transfer_date'] = new_dates
    df_bank['amount'] = n_amounts

    #convert dates to datetime format
    df_bank.transfer_date = pd.to_datetime(arg=df_bank.transfer_date, errors='raise', format="%d.%m.%Y")

    #sort by date
    df_bank.sort_values(by=['transfer_date'])

    return df_bank


def get_payouts(df):
    '''


    '''
    payouts = df[df['item_type'] == 'payout']
    payouts = payouts.drop(columns=['item_name', 'artist', 'item_price', 'quantity', 'sub_total', 'marketplace_tax', 'shipping',\
        'fee_type','assessed_revenue_share', 'collected_revenue_share', 'balance_of_revenue_share_(EUR)', 'balance_of_revenue_share_(CHF)',\
            'net_amount'])
    return payouts
