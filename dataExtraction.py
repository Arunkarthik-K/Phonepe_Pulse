import os
import json
import pandas as pd
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine


def extractData():
    # Extract aggregated transaction data
    path_1 = "/Users/arunkarthik/Projects/Personal/phonepe/Dataset/data/aggregated/transaction/country/india/state/"
    agg_tran_state_list = os.listdir(path_1)

    agg_tra = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [],
               'Transaction_amount': []}

    for i in agg_tran_state_list:
        state_1 = path_1 + i + "/"
        agg_yr_1 = os.listdir(state_1)

        for j in agg_yr_1:
            year_1 = state_1 + j + "/"
            agg_yr_list_1 = os.listdir(year_1)

            for k in agg_yr_list_1:
                pk_1 = year_1 + k
                data_1 = open(pk_1, 'r')
                rawdata_1 = json.load(data_1)

                if rawdata_1['data']['transactionData']:
                    for x in rawdata_1['data']['transactionData']:
                        name = x['name']
                        count = x['paymentInstruments'][0]['count']
                        amount = x['paymentInstruments'][0]['amount']
                        agg_tra['State'].append(i)
                        agg_tra['Year'].append(j)
                        agg_tra['Quarter'].append(int(k.strip('.json')))
                        agg_tra['Transaction_type'].append(name)
                        agg_tra['Transaction_count'].append(count)
                        agg_tra['Transaction_amount'].append(amount)
                else:
                    pass

    df_aggregated_transaction = pd.DataFrame(agg_tra)

    # Extract aggregated user data
    path_2 = "/Users/arunkarthik/Projects/Personal/phonepe/Dataset/data/aggregated/user/country/india/state/"
    agg_user_state_list = os.listdir(path_2)

    agg_user = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'User_Count': [], 'User_Percentage': []}

    for i in agg_user_state_list:
        state_2 = path_2 + i + "/"
        agg_yr_2 = os.listdir(state_2)

        for j in agg_yr_2:
            year_2 = state_2 + j + "/"
            agg_yr_list_2 = os.listdir(year_2)

            for k in agg_yr_list_2:
                pk_2 = year_2 + k
                data_2 = open(pk_2, 'r')
                rawdata_2 = json.load(data_2)

                if rawdata_2["data"]["usersByDevice"]:
                    for y in rawdata_2["data"]["usersByDevice"]:
                        brand_name = y["brand"]
                        count_ = y["count"]
                        all_percentage = y["percentage"]
                        agg_user["State"].append(i)
                        agg_user["Year"].append(j)
                        agg_user["Quarter"].append(int(k.strip('.json')))
                        agg_user["Brands"].append(brand_name)
                        agg_user["User_Count"].append(count_)
                        agg_user["User_Percentage"].append(all_percentage * 100)
                else:
                    pass

    df_aggregated_user = pd.DataFrame(agg_user)

    # Extract map transaction data
    path_3 = "/Users/arunkarthik/Projects/Personal/phonepe/Dataset/data/map/transaction/hover/country/india/state/"
    map_tra_state_list = os.listdir(path_3)

    map_tra = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Transaction_Count': [],
               'Transaction_Amount': []}

    for i in map_tra_state_list:
        state_3 = path_3 + i + "/"
        agg_yr_3 = os.listdir(state_3)

        for j in agg_yr_3:
            year_3 = state_3 + j + "/"
            agg_yr_list_3 = os.listdir(year_3)

            for k in agg_yr_list_3:
                pk_3 = year_3 + k
                data_3 = open(pk_3, 'r')
                rawdata_3 = json.load(data_3)

                if rawdata_3["data"]["hoverDataList"]:
                    for z in rawdata_3["data"]["hoverDataList"]:
                        district = z["name"]
                        count = z["metric"][0]["count"]
                        amount = z["metric"][0]["amount"]
                        map_tra['State'].append(i)
                        map_tra['Year'].append(j)
                        map_tra['Quarter'].append(int(k.strip('.json')))
                        map_tra["District"].append(district)
                        map_tra["Transaction_Count"].append(count)
                        map_tra["Transaction_Amount"].append(amount)
                else:
                    pass

    df_map_transaction = pd.DataFrame(map_tra)

    # Extract map user data
    path_4 = "/Users/arunkarthik/Projects/Personal/phonepe/Dataset/data/map/user/hover/country/india/state/"
    map_user_state_list = os.listdir(path_4)

    map_user = {"State": [], "Year": [], "Quarter": [], "District": [], "Registered_User": []}

    for i in map_user_state_list:
        state_4 = path_4 + i + "/"
        agg_yr_4 = os.listdir(state_4)

        for j in agg_yr_4:
            year_4 = state_4 + j + "/"
            agg_yr_list_4 = os.listdir(year_4)

            for k in agg_yr_list_4:
                pk_4 = year_4 + k
                data_4 = open(pk_4, 'r')
                rawdata_4 = json.load(data_4)

                if rawdata_4["data"]["hoverData"]:
                    for m in rawdata_4["data"]["hoverData"].items():
                        district = m[0]
                        registered_user = m[1]["registeredUsers"]
                        map_user['State'].append(i)
                        map_user['Year'].append(j)
                        map_user['Quarter'].append(int(k.strip('.json')))
                        map_user["District"].append(district)
                        map_user["Registered_User"].append(registered_user)
                else:
                    pass

    df_map_user = pd.DataFrame(map_user)

    # Extract top transaction data
    path_5 = "/Users/arunkarthik/Projects/Personal/phonepe/Dataset/data/top/transaction/country/india/state/"
    top_tra_state_list = os.listdir(path_5)

    top_tra = {'State': [], 'Year': [], 'Quarter': [], 'District_Pincode': [], 'Transaction_count': [],
               'Transaction_amount': []}

    for i in top_tra_state_list:
        state_5 = path_5 + i + "/"
        agg_yr_5 = os.listdir(state_5)

        for j in agg_yr_5:
            year_5 = state_5 + j + "/"
            agg_yr_list_5 = os.listdir(year_5)

            for k in agg_yr_list_5:
                pk_5 = year_5 + k
                data_5 = open(pk_5, 'r')
                rawdata_5 = json.load(data_5)

                if rawdata_5['data']['pincodes']:
                    for n in rawdata_5['data']['pincodes']:
                        name = n['entityName']
                        count = n['metric']['count']
                        amount = n['metric']['amount']
                        top_tra['State'].append(i)
                        top_tra['Year'].append(j)
                        top_tra['Quarter'].append(int(k.strip('.json')))
                        top_tra['District_Pincode'].append(name)
                        top_tra['Transaction_count'].append(count)
                        top_tra['Transaction_amount'].append(amount)
                else:
                    pass

    df_top_transaction = pd.DataFrame(top_tra)

    # Extract top user data
    path_6 = "/Users/arunkarthik/Projects/Personal/phonepe/Dataset/data/top/user/country/india/state/"
    top_user_state_list = os.listdir(path_6)

    top_user = {'State': [], 'Year': [], 'Quarter': [], 'District_Pincode': [], 'Registered_User': []}

    for i in top_user_state_list:
        state_6 = path_6 + i + "/"
        agg_yr_6 = os.listdir(state_6)

        for j in agg_yr_6:
            year_6 = state_6 + j + "/"
            agg_yr_list_6 = os.listdir(year_6)

            for k in agg_yr_list_6:
                pk_6 = year_6 + k
                data_6 = open(pk_6, 'r')
                rawdata_6 = json.load(data_6)

                if rawdata_6['data']['pincodes']:
                    for p in rawdata_6['data']['pincodes']:
                        name = p['name']
                        registered_user = p['registeredUsers']
                        top_user['State'].append(i)
                        top_user['Year'].append(j)
                        top_user['Quarter'].append(int(k.strip('.json')))
                        top_user['District_Pincode'].append(name)
                        top_user['Registered_User'].append(registered_user)
                else:
                    pass

    df_top_user = pd.DataFrame(top_user)

    # Connect to the MySQL server
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Password123"
    )

    # Create a new database and use
    my_cursor = mydb.cursor()
    my_cursor.execute("CREATE DATABASE IF NOT EXISTS phonepe")

    # Close the cursor and database connection
    my_cursor.close()
    mydb.close()

    # Connect to the new created database
    engine = create_engine('mysql+mysqlconnector://root:Password123@localhost/phonepe', echo=False)

    df_aggregated_transaction.to_sql('aggregated_transaction', engine, if_exists='replace', index=False,
                                     dtype={'State': sqlalchemy.types.VARCHAR(length=50),
                                            'Year': sqlalchemy.types.Integer,
                                            'Quarter': sqlalchemy.types.Integer,
                                            'Transaction_type': sqlalchemy.types.VARCHAR(length=50),
                                            'Transaction_count': sqlalchemy.types.Integer,
                                            'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

    df_aggregated_user.to_sql('aggregated_user', engine, if_exists='replace', index=False,
                              dtype={'State': sqlalchemy.types.VARCHAR(length=50),
                                     'Year': sqlalchemy.types.Integer,
                                     'Quarter': sqlalchemy.types.Integer,
                                     'Brands': sqlalchemy.types.VARCHAR(length=50),
                                     'User_Count': sqlalchemy.types.Integer,
                                     'User_Percentage': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

    df_map_transaction.to_sql('map_transaction', engine, if_exists='replace', index=False,
                              dtype={'State': sqlalchemy.types.VARCHAR(length=50),
                                     'Year': sqlalchemy.types.Integer,
                                     'Quarter': sqlalchemy.types.Integer,
                                     'District': sqlalchemy.types.VARCHAR(length=50),
                                     'Transaction_Count': sqlalchemy.types.Integer,
                                     'Transaction_Amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

    df_map_user.to_sql('map_user', engine, if_exists='replace', index=False,
                       dtype={'State': sqlalchemy.types.VARCHAR(length=50),
                              'Year': sqlalchemy.types.Integer,
                              'Quarter': sqlalchemy.types.Integer,
                              'District': sqlalchemy.types.VARCHAR(length=50),
                              'Registered_User': sqlalchemy.types.Integer, })

    df_top_transaction.to_sql('top_transaction', engine, if_exists='replace', index=False,
                              dtype={'State': sqlalchemy.types.VARCHAR(length=50),
                                     'Year': sqlalchemy.types.Integer,
                                     'Quarter': sqlalchemy.types.Integer,
                                     'District_Pincode': sqlalchemy.types.Integer,
                                     'Transaction_count': sqlalchemy.types.Integer,
                                     'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

    df_top_user.to_sql('top_user', engine, if_exists='replace', index=False,
                       dtype={'State': sqlalchemy.types.VARCHAR(length=50),
                              'Year': sqlalchemy.types.Integer,
                              'Quarter': sqlalchemy.types.Integer,
                              'District_Pincode': sqlalchemy.types.Integer,
                              'Registered_User': sqlalchemy.types.Integer, })
