import pandas as pd
import json
import os
import mysql.connector
import sqlite3
import numpy as np

#fetcing data from github and creating data frame
path="D:/phonepeproject/pulse/data/aggregated/transaction/country/india/state/"
Agg_state_list=os.listdir(path)
column={'State':[], 'Year':[],'Quater':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}
for i in Agg_state_list:
    states=path+i+"/"
    Agg_yr=os.listdir(states)
    for j in Agg_yr:
        years=states+j+"/"
        Agg_yr_list=os.listdir(years)
        for k in Agg_yr_list:
            files=years+k
            Data=open(files,'r')


            
            D=json.load(Data)
            for z in D['data']['transactionData']:
              Name=z['name']
              count=z['paymentInstruments'][0]['count']
              amount=z['paymentInstruments'][0]['amount']
              column['Transacion_type'].append(Name)
              column['Transacion_count'].append(count)
              column['Transacion_amount'].append(amount)
              column['State'].append(i)
              column['Year'].append(j)
              column['Quater'].append(int(k.strip('.json')))
Agg_user=pd.DataFrame(column)

Agg_user['State']=Agg_user['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Agg_user['State']=Agg_user['State'].str.replace('-',' ')
Agg_user['State']=Agg_user['State'].str.title()
Agg_user['State']=Agg_user['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')


path1="D:/phonepeproject/pulse/data/aggregated/user/country/india/state/"

column1={'State':[], 'Year':[],'Quater':[],'Brand':[], 'Transacion_count':[], 'Percentage':[]}
for i in Agg_state_list:
    states=path1+i+"/"
    Agg_yr=os.listdir(states)
    for j in Agg_yr:
        years=states+j+"/"
        Agg_yr_list=os.listdir(years)
        for k in Agg_yr_list:
            files=years+k
            Data=open(files,'r')
            D1=json.load(Data)
            try:    
                for z in D1['data']['usersByDevice']:
                    brand=z['brand']
                    count=z['count']
                    percentage=z['percentage']
                    column1['Brand'].append(brand)
                    column1['Transacion_count'].append(count)
                    column1['Percentage'].append(percentage)
                    column1['State'].append(i)
                    column1['Year'].append(j)
                    column1['Quater'].append(int(k.strip('.json')))
            except:
                pass    
Agg_user=pd.DataFrame(column1)

Agg_user['State']=Agg_user['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Agg_user['State']=Agg_user['State'].str.replace('-',' ')
Agg_user['State']=Agg_user['State'].str.title()
Agg_user['State']=Agg_user['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')


path2="D:/phonepeproject/pulse/data/map/transaction/hover/country/india/state/"

column2={'State':[], 'Year':[],'Quater':[],'District_name':[], 'Transacion_count':[], 'Trans_amount':[]}
for i in Agg_state_list:
    states=path2+i+"/"
    Agg_yr=os.listdir(states)
    for j in Agg_yr:
        years=states+j+"/"
        Agg_yr_list=os.listdir(years)
        for k in Agg_yr_list:
            files=years+k
            Data=open(files,'r')
            D2=json.load(Data)
            try:    
                for Z in D2["data"]["hoverDataList"]:

                    name=Z["name"]
                    amount=Z["metric"][0]["amount"]
                    count=Z["metric"][0]["count"]
                    column2['District_name'].append(name)
                    column2['Transacion_count'].append(count)
                    column2['Trans_amount'].append(amount)
                    column2['State'].append(i)
                    column2['Year'].append(j)
                    column2['Quater'].append(int(k.strip('.json')))
            except:
                pass    
Map_trans=pd.DataFrame(column2)

Map_trans['State']=Map_trans['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Map_trans['State']=Map_trans['State'].str.replace('-',' ')
Map_trans['State']=Map_trans['State'].str.title()
Map_trans['State']=Map_trans['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')



path3="D:/phonepeproject/pulse/data/map/user/hover/country/india/state/"

column3={'State':[], 'Year':[],'Quater':[],'District_name':[], 'Registerd_user':[], 'App_open_count':[]}
for i in Agg_state_list:
    states=path3+i+"/"
    Agg_yr=os.listdir(states)
    for j in Agg_yr:
        years=states+j+"/"
        Agg_yr_list=os.listdir(years)
        for k in Agg_yr_list:
            files=years+k
            Data=open(files,'r')
            D3=json.load(Data)
            try:
                for Z in D3["data"]["hoverData"].items():
                    district=Z[0]
                    regis_user=Z[1]["registeredUsers"]
                    open_count=Z[1]["appOpens"]
                    column3['District_name'].append(district)
                    column3['Registerd_user'].append(regis_user)
                    column3['App_open_count'].append(open_count)
                    column3['State'].append(i)
                    column3['Year'].append(j)
                    column3['Quater'].append(int(k.strip('.json')))
            except:
                pass    
Map_user=pd.DataFrame(column3)

Map_user['State']=Map_user['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Map_user['State']=Map_user['State'].str.replace('-',' ')
Map_user['State']=Map_user['State'].str.title()
Map_user['State']=Map_user['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')


path4="D:/phonepeproject/pulse/data/top/transaction/country/india/state/"

column4={'State':[], 'Year':[],'Quater':[],'District_pincode':[], 'Trans_count':[], 'Trans_amount':[]}
for i in Agg_state_list:
    states=path4+i+"/"
    Agg_yr=os.listdir(states)
    for j in Agg_yr:
        years=states+j+"/"
        Agg_yr_list=os.listdir(years)
        for k in Agg_yr_list:
            files=years+k
            Data=open(files,'r')
            D4=json.load(Data)
            try:
                for Z in D4["data"]['pincodes']:
                    district=Z['entityName']
                    count=Z['metric']['count']
                    amount=Z['metric']['amount']
                    column4['District_pincode'].append(district)
                    column4['Trans_count'].append(count)
                    column4['Trans_amount'].append(amount)
                    column4['State'].append(i)
                    column4['Year'].append(j)
                    column4['Quater'].append(int(k.strip('.json')))
            except:
                pass    
Top_Trans=pd.DataFrame(column4)

Top_Trans['State']=Top_Trans['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Top_Trans['State']=Top_Trans['State'].str.replace('-',' ')
Top_Trans['State']=Top_Trans['State'].str.title()
Top_Trans['State']=Top_Trans['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')


path5="D:/phonepeproject/pulse/data/top/user/country/india/state/"

column5={'State':[], 'Year':[],'Quater':[],'District_pincode':[], 'Reg_user_count':[]}
for i in Agg_state_list:
    states=path5+i+"/"
    Agg_yr=os.listdir(states)
    for j in Agg_yr:
        years=states+j+"/"
        Agg_yr_list=os.listdir(years)
        for k in Agg_yr_list:
            files=years+k
            Data=open(files,'r')
            D5=json.load(Data)
            try:
               for Z in D5["data"]['pincodes']:
                    district=Z['name']
                    reg_user=Z['registeredUsers']
                    column5['District_pincode'].append(district)
                    column5['Reg_user_count'].append(reg_user)
                    column5['State'].append(i)
                    column5['Year'].append(j)
                    column5['Quater'].append(int(k.strip('.json')))
            except:
                pass    
Top_user=pd.DataFrame(column5)
Top_user['State']=Top_user['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Top_user['State']=Top_user['State'].str.replace('-',' ')
Top_user['State']=Top_user['State'].str.title()
Top_user['State']=Top_user['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')


path6="D:/phonepeproject/pulse/data/top/transaction/country/india/state/"

column6={'State':[], 'Year':[],'Quater':[],'District_name':[], 'Trans_count':[], 'Trans_amount':[]}
for i in Agg_state_list:
    states=path6+i+"/"
    Agg_yr=os.listdir(states)
    for j in Agg_yr:
        years=states+j+"/"
        Agg_yr_list=os.listdir(years)
        for k in Agg_yr_list:
            files=years+k
            Data=open(files,'r')
            D6=json.load(Data)
            try:
                for Z in D6["data"]['districts']:
                    district=Z['entityName']
                    count=Z['metric']['count']
                    amount=Z['metric']['amount']
                    column6['District_name'].append(district)
                    column6['Trans_count'].append(count)
                    column6['Trans_amount'].append(amount)
                    column6['State'].append(i)
                    column6['Year'].append(j)
                    column6['Quater'].append(int(k.strip('.json')))
            except:
                pass    
Top_Trans_district=pd.DataFrame(column6)

Top_Trans_district['State']=Top_Trans_district['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Top_Trans_district['State']=Top_Trans_district['State'].str.replace('-',' ')
Top_Trans_district['State']=Top_Trans_district['State'].str.title()
Top_Trans_district['State']=Top_Trans_district['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')


path7="D:/phonepeproject/pulse/data/top/user/country/india/state/"

column7={'State':[], 'Year':[],'Quater':[],'District_name':[], 'Reg_user_count':[]}
for i in Agg_state_list:
    states=path7+i+"/"
    Agg_yr=os.listdir(states)
    for j in Agg_yr:
        years=states+j+"/"
        Agg_yr_list=os.listdir(years)
        for k in Agg_yr_list:
            files=years+k
            Data=open(files,'r')
            D7=json.load(Data)
            try:
                for Z in D7["data"]['districts']:
                    district=Z['name']
                    reg_user=Z['registeredUsers']
                    column7['District_name'].append(district)
                    column7['Reg_user_count'].append(reg_user)
                    column7['State'].append(i)
                    column7['Year'].append(j)
                    column7['Quater'].append(int(k.strip('.json')))
            except:
                pass    
Top_user_district=pd.DataFrame(column7)

Top_user_district['State']=Top_user_district['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Top_user_district['State']=Top_user_district['State'].str.replace('-',' ')
Top_user_district['State']=Top_user_district['State'].str.title()
Top_user_district['State']=Top_user_district['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')


#migrating data to sql



config = {
    'user':'root', 'password':'Ansiya93',
    'host':'127.0.0.1', 'database':'phonepe'}

print(config)
connection = mysql.connector.connect(**config)
cursor=connection.cursor()
print(cursor)
# drop_query="""DROP TABLE if exists aggregate_transaction"""
# cursor.execute(drop_query)
# connection.commit()
create_table= """CREATE TABLE if not exists aggregate_transaction(State VARCHAR(100),Year INT,Quater BIGINT,
Transacion_type VARCHAR(100),Transacion_count BIGINT(255),Transacion_amount FLOAT)"""
cursor.execute(create_table)
connection.commit()

from sqlalchemy import create_engine
con = create_engine("mysql+mysqlconnector://root:Ansiya93@localhost/phonepe")



oldaggr_trans=pd.read_sql_query("SELECT * FROM aggregate_transaction", con)
newaggr_trans=pd.concat([oldaggr_trans, Agg_user], ignore_index=True)
newaggr_trans.drop_duplicates(inplace=True)
newaggr_trans.to_sql('aggregate_transaction', con, if_exists='replace', index=False)




config = {
    'user':'root', 'password':'Ansiya93',
    'host':'127.0.0.1', 'database':'phonepe'}

print(config)
connection = mysql.connector.connect(**config)
cursor=connection.cursor()
print(cursor)
# drop_query="""DROP TABLE if exists aggregate_user"""
# cursor.execute(drop_query)
# connection.commit()
create_table= """CREATE TABLE if not exists aggregate_user(State VARCHAR(100),Year INT,Quater BIGINT,
Brand VARCHAR(100),Transacion_count BIGINT(255),Percentage FLOAT)"""
cursor.execute(create_table)
connection.commit()

from sqlalchemy import create_engine
con = create_engine("mysql+mysqlconnector://root:Ansiya93@localhost/phonepe")



oldaggr_user=pd.read_sql_query("SELECT * FROM aggregate_user", con)
newaggr_user=pd.concat([oldaggr_user, Agg_user], ignore_index=True)
newaggr_user.drop_duplicates(inplace=True)
newaggr_user.to_sql('aggregate_user', con, if_exists='replace', index=False)




config = {
    'user':'root', 'password':'Ansiya93',
    'host':'127.0.0.1', 'database':'phonepe'}

print(config)
connection = mysql.connector.connect(**config)
cursor=connection.cursor()
print(cursor)
# drop_query="""DROP TABLE if exists map_transaction"""
# cursor.execute(drop_query)
# connection.commit()
create_table= """CREATE TABLE if not exists map_transaction(State VARCHAR(100),Year INT,Quater BIGINT,
District_name VARCHAR(100),Transacion_count BIGINT(255),Trans_amount FLOAT)"""
cursor.execute(create_table)
connection.commit()

from sqlalchemy import create_engine
con = create_engine("mysql+mysqlconnector://root:Ansiya93@localhost/phonepe")



oldmap_trans=pd.read_sql_query("SELECT * FROM map_transaction", con)
newmap_trans=pd.concat([oldmap_trans, Map_trans], ignore_index=True)
newmap_trans.drop_duplicates(inplace=True)
newmap_trans.to_sql('map_transaction', con, if_exists='replace', index=False)


config = {
    'user':'root', 'password':'Ansiya93',
    'host':'127.0.0.1', 'database':'phonepe'}

print(config)
connection = mysql.connector.connect(**config)
cursor=connection.cursor()
print(cursor)
# drop_query="""DROP TABLE if exists map_user"""
# cursor.execute(drop_query)
# connection.commit()
create_table= """CREATE TABLE if not exists map_user(State VARCHAR(100),Year INT,Quater BIGINT,
District_name VARCHAR(100),Registerd_user BIGINT(255),App_open_count BIGINT)"""
cursor.execute(create_table)
connection.commit()

from sqlalchemy import create_engine
con = create_engine("mysql+mysqlconnector://root:Ansiya93@localhost/phonepe")



oldmap_user=pd.read_sql_query("SELECT * FROM map_user", con)
newmap_user=pd.concat([oldmap_user, Map_user], ignore_index=True)
newmap_user.drop_duplicates(inplace=True)
newmap_user.to_sql('map_user', con, if_exists='replace', index=False)




config = {
    'user':'root', 'password':'Ansiya93',
    'host':'127.0.0.1', 'database':'phonepe'}

print(config)
connection = mysql.connector.connect(**config)
cursor=connection.cursor()
print(cursor)
# drop_query="""DROP TABLE if exists top_transaction"""
# cursor.execute(drop_query)
# connection.commit()
create_table= """CREATE TABLE if not exists top_transaction(State VARCHAR(100),Year INT,Quater BIGINT,
District_pincode BIGINT,Trans_count BIGINT,Trans_amount FLOAT)"""
cursor.execute(create_table)
connection.commit()

from sqlalchemy import create_engine
con = create_engine("mysql+mysqlconnector://root:Ansiya93@localhost/phonepe")



oldTop_trans=pd.read_sql_query("SELECT * FROM top_transaction", con)
newTop_trans=pd.concat([oldTop_trans, Top_Trans], ignore_index=True)
newTop_trans.drop_duplicates(inplace=True)
newTop_trans.to_sql('top_transaction', con, if_exists='replace', index=False)




config = {
    'user':'root', 'password':'Ansiya93',
    'host':'127.0.0.1', 'database':'phonepe'}

print(config)
connection = mysql.connector.connect(**config)
cursor=connection.cursor()
print(cursor)
# drop_query="""DROP TABLE if exists top_user"""
# cursor.execute(drop_query)
# connection.commit()
create_table= """CREATE TABLE if not exists top_user(State VARCHAR(100),Year INT,Quater BIGINT,
District_pincode BIGINT,Reg_user_count BIGINT)"""
cursor.execute(create_table)
connection.commit()

from sqlalchemy import create_engine
con = create_engine("mysql+mysqlconnector://root:Ansiya93@localhost/phonepe")



oldTop_user=pd.read_sql_query("SELECT * FROM top_user", con)
newTop_user=pd.concat([oldTop_user, Top_user], ignore_index=True)
newTop_user.drop_duplicates(inplace=True)
newTop_user.to_sql('top_user', con, if_exists='replace', index=False)




config = {
    'user':'root', 'password':'Ansiya93',
    'host':'127.0.0.1', 'database':'phonepe'}

print(config)
connection = mysql.connector.connect(**config)
cursor=connection.cursor()
print(cursor)
# drop_query="""DROP TABLE if exists top_trans_district"""
# cursor.execute(drop_query)
# connection.commit()
create_table= """CREATE TABLE if not exists top_trans_district(State VARCHAR(100),Year INT,Quater BIGINT,
District_name VARCHAR(100),Trans_count BIGINT,Trans_amount FLOAT)"""
cursor.execute(create_table)
connection.commit()

from sqlalchemy import create_engine
con = create_engine("mysql+mysqlconnector://root:Ansiya93@localhost/phonepe")



oldTop_trans_dis=pd.read_sql_query("SELECT * FROM top_trans_district", con)
newTop_trans_dis=pd.concat([oldTop_trans_dis, Top_Trans_district], ignore_index=True)
newTop_trans_dis.drop_duplicates(inplace=True)
newTop_trans_dis.to_sql('top_trans_district', con, if_exists='replace', index=False)




config = {
    'user':'root', 'password':'Ansiya93',
    'host':'127.0.0.1', 'database':'phonepe'}

print(config)
connection = mysql.connector.connect(**config)
cursor=connection.cursor()
print(cursor)
# drop_query="""DROP TABLE if exists top_user_district"""
# cursor.execute(drop_query)
# connection.commit()
create_table= """CREATE TABLE if not exists top_user_district(State VARCHAR(100),Year INT,Quater BIGINT,
District_name VARCHAR(100),Reg_user_count BIGINT)"""
cursor.execute(create_table)
connection.commit()

from sqlalchemy import create_engine
con = create_engine("mysql+mysqlconnector://root:Ansiya93@localhost/phonepe")



oldTop_user_district=pd.read_sql_query("SELECT * FROM top_user_district", con)
newTop_user_district=pd.concat([oldTop_user_district, Top_user_district], ignore_index=True)
newTop_user_district.drop_duplicates(inplace=True)
newTop_user_district.to_sql('top_user_district', con, if_exists='replace', index=False)



