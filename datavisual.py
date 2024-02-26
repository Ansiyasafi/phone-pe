import streamlit as st
import mysql.connector
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import requests
import json
config = {
    'user':'root', 'password':'Ansiya93',
    'host':'127.0.0.1', 'database':'phonepe'}

print(config)
connection = mysql.connector.connect(**config)
cursor=connection.cursor()
print(cursor)

st.set_page_config(layout='wide')

# Title
st.header(':violet[Phonepe Pulse Data Visualization ]')
st.write('**(Note)**:-This data between **2018** to **2023** in **INDIA**')
option = st.radio('**Select your option**',('All India','Top  categories'),horizontal=True)
if option == 'All India':

    # Select tab
    tab1, tab2 ,tab3,tab4= st.tabs(['Aggregated Transaction','Map Transaction','Aggregated User','Map user'])

    # -------------------------       /     All India Transaction        /        ------------------ #
    with tab1:
        col1, col2, col3 = st.columns(3)
        with col1:
            trans_year = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='trans_year')
        with col2:
            trans_quatr = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='trans_quatr')
        with col3:
            trans_type = st.selectbox('**Select Transaction type**', ('Recharge & bill payments','Peer-to-peer payments',
            'Merchant payments','Financial Services','Others'),key='trans_type')

        cursor.execute(f"SELECT State, Transacion_amount FROM aggregate_transaction WHERE Year = '{trans_year}' AND Quater = '{trans_quatr}' AND Transacion_type = '{trans_type}';")
        tran_query = cursor.fetchall()
        df_tran_query = pd.DataFrame(np.array(tran_query), columns=['State', 'Transaction_amount'])
        df_tran_query1 = df_tran_query.set_index(pd.Index(range(1, len(df_tran_query)+1)))


        cursor.execute(f"SELECT State, Transacion_count, Transacion_amount FROM aggregate_transaction WHERE Year = '{trans_year}' AND Quater = '{trans_quatr}' AND Transacion_type = '{trans_type}';")
        trans_analys_query = cursor.fetchall()
        df_trans_analys_query = pd.DataFrame(np.array(trans_analys_query), columns=['State','Transaction_count','Transaction_amount'])
        df_trans_analys_query1 = df_trans_analys_query.set_index(pd.Index(range(1, len(df_trans_analys_query)+1)))

        

        cursor.execute(f"SELECT SUM(Transacion_amount), AVG(Transacion_amount) FROM aggregate_transaction WHERE Year = '{trans_year}' AND Quater = '{trans_quatr}' AND Transacion_type = '{trans_type}';")
        trans_amnt_query = cursor.fetchall()
        df_trans_amnt_query = pd.DataFrame(np.array(trans_amnt_query), columns=['Total','Average'])
        df_trans_amnt_query1 = df_trans_amnt_query.set_index(['Average'])
        
        # Total Transaction Count table query
        cursor.execute(f"SELECT SUM(Transacion_count), AVG(Transacion_count) FROM aggregate_transaction WHERE Year = '{trans_year}' AND Quater = '{trans_quatr}' AND Transacion_type = '{trans_type}';")
        trans_count_query = cursor.fetchall()
        df_trans_count_query = pd.DataFrame(np.array(trans_count_query), columns=['Total','Average'])
        df_trans_count_query1 = df_trans_count_query.set_index(['Average'])

        # Clone the gio data
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        data1 = json.loads(response.content)
        state_names_tra = [feature['properties']['ST_NM'] for feature in data1['features']]
        state_names_tra.sort()
       
       #geo visualization of transaction amount

        df =  df_tran_query
        fig = px.choropleth(
            df,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='State',
            color='Transaction_amount',
            color_continuous_scale='Reds'
        )

        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(title="transaction amount",title_font=dict(size=33),title_font_color='#6739b7', height=800)
        st.plotly_chart(fig,use_container_width=True)
        #transaction bar chart
        
        df_tran_query1['State'] = df_tran_query1['State'].astype(str)
        df_tran_query1['Transaction_amount'] = df_tran_query1['Transaction_amount'].astype(float)
        df_tran_query1_fig = px.bar(df_tran_query1 , x = 'State', y ='Transaction_amount', color ='Transaction_amount', color_continuous_scale = 'thermal', title = 'Transaction Analysis Chart', height = 700,)
        df_tran_query1_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
        st.plotly_chart(df_tran_query1_fig,use_container_width=True)

        
        

         # -------  /  All India Total Transaction calculation Table   /   ----  #
        st.header(':violet[Total calculation]')

        col4, col5 = st.columns(2)
        with col4:
            st.subheader(':red[Transaction Analysis]')
            st.dataframe(df_trans_analys_query1)
        with col5:
            st.subheader(':red[Transaction Amount]')
            st.dataframe(df_trans_amnt_query1)
            st.subheader(':red[Transaction Count]')
            st.dataframe(df_trans_count_query1)

    #----------------------------map transaction------------------------#        
    with tab2:
        col1, col2 = st.columns(2)
        with col1:
            trans_year1 = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='trans_year1')
        with col2:
            trans_quatr1 = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='trans_quatr1')

        cursor.execute(f"SELECT State, District_name,Transacion_count FROM map_transaction WHERE Year='{trans_year1}' AND Quater ='{trans_quatr1}';")
        map_trans_query=cursor.fetchall()
        df_map_trans_query=pd.DataFrame(np.array(map_trans_query),columns=["State",'District_name','Transaction_count'])
        df_map_trans_query1 = df_map_trans_query.set_index(pd.Index(range(1, len(df_map_trans_query)+1)))  

        # Clone the gio data
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        data1 = json.loads(response.content)

        df =  df_map_trans_query
        fig1 = px.choropleth(
            df,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='State',
            color='Transaction_count',
            hover_name='District_name',
            color_continuous_scale='Reds'
        )

        fig1.update_geos(fitbounds="locations", visible=False)
        fig1.update_layout(title="transaction count",title_font=dict(size=33),title_font_color='#6739b7', height=500)
        st.plotly_chart(fig1,use_container_width=True)  
           
        #transaction bar chart
        df_map_trans_query1['State'] = df_map_trans_query1['State'].astype(str)
        # df_map_trans_query1['Transaction_count'] = df_map_trans_query1['Transaction_count'].astype(float)
        df_map_trans_query1_fig = px.bar(df_map_trans_query1 , x = 'State', y ='Transaction_count', color ='Transaction_count', color_continuous_scale = 'thermal', title = 'Transaction Analysis Chart', height = 700,)
        df_map_trans_query1_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
        st.plotly_chart(df_map_trans_query1_fig,use_container_width=True)
    
#------------------tab 3 aggregated user-----------------------------#

    with tab3:
        col1, col2 = st.columns(2)
        with col1:
            user_year = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022'),key='user_year')
        with col2:
            user_quatr = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='user_quatr')

        cursor.execute(f"SELECT State, Brand,Transacion_count FROM aggregate_user WHERE Year = '{ user_year}' AND Quater = '{user_quatr}';")
        aggre_user_qry_rslt = cursor.fetchall()
        aggre_user_qry_rslt = pd.DataFrame(np.array(aggre_user_qry_rslt), columns=['State', 'Brand','Transaction_count'])
        aggre_user_qry_rslt1 = aggre_user_qry_rslt.set_index(pd.Index(range(1, len(aggre_user_qry_rslt)+1)))

        cursor.execute(f"SELECT State, SUM(Transacion_Count) FROM aggregate_user WHERE Year = '{ user_year}' AND Quater = '{user_quatr}' GROUP BY State;")
        aggre_user_count_qry_rslt = cursor.fetchall()
        aggre_user_count_qry_rslt = pd.DataFrame(np.array(aggre_user_count_qry_rslt), columns=['State', 'Transaction_count'])
        aggre_user_count_qry_rslt1 = aggre_user_count_qry_rslt.set_index(pd.Index(range(1, len(aggre_user_count_qry_rslt)+1)))    

        cursor.execute(f"SELECT  SUM(Transacion_Count),AVG(Transacion_Count) FROM aggregate_user WHERE Year = '{ user_year}' AND Quater = '{user_quatr}';")
        aggre_user_count1_qry_rslt = cursor.fetchall()
        aggre_user_count1_qry_rslt = pd.DataFrame(np.array(aggre_user_count1_qry_rslt), columns=['Total', 'Average'])
        aggre_user_count1_qry_rslt1 = aggre_user_count1_qry_rslt.set_index(['Average']) 
#------------------------geo visualization------------#
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        data1 = json.loads(response.content)

        df =  aggre_user_qry_rslt
        fig2 = px.choropleth(
            df,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='State',
            color='Brand',
            hover_name='Transaction_count',
            color_continuous_scale='Reds'
        )

        fig2.update_geos(fitbounds="locations", visible=False)
        fig2.update_layout(title="User transaction count",title_font=dict(size=33),title_font_color='#6739b7', height=500)
        st.plotly_chart(fig2,use_container_width=True) 

         #transaction bar chart
        
        aggre_user_count_qry_rslt1 ['State'] = aggre_user_count_qry_rslt1 ['State'].astype(str)
        aggre_user_count_qry_rslt1_fig = px.bar(aggre_user_count_qry_rslt1  , x = 'State', y ='Transaction_count', color ='Transaction_count', color_continuous_scale = 'thermal', title = 'Transaction User Analysis Chart', height = 700,)
        aggre_user_count_qry_rslt1_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
        st.plotly_chart(aggre_user_count_qry_rslt1_fig,use_container_width=True) 


         # -----   /   All India Total User calculation Table   /   ----- #
        st.header(':violet[Total calculation]')

        col3, col4 = st.columns(2)
        with col3:
            st.subheader('User Analysis')
            st.dataframe(aggre_user_count_qry_rslt1)
        with col4:
            st.subheader('User Count')
            st.dataframe(aggre_user_count1_qry_rslt1)  


#---------------------tab 4--map user----------------------------------#
    with tab4:
        col1, col2 = st.columns(2)
        with col1:
            user1_year = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='user1_year')
        with col2:
            user1_quatr = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='user1_quatr')

        cursor.execute(f"SELECT State,App_open_count,Registerd_user FROM map_user WHERE Year = '{ user1_year}' AND Quater = '{user1_quatr}';")
        map_user_qry_rslt = cursor.fetchall()
        map_user_qry_rslt = pd.DataFrame(np.array(map_user_qry_rslt), columns=['State','App_open_count','Registerd_user'])
        map_user_qry_rslt1 = map_user_qry_rslt.set_index(pd.Index(range(1, len(map_user_qry_rslt)+1)))

        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        data1 = json.loads(response.content)

        df =  map_user_qry_rslt
        fig3 = px.choropleth(
            df,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='State',
            color='Registerd_user',
            hover_name='App_open_count',
            color_continuous_scale='Reds'
        )

        fig3.update_geos(fitbounds="locations", visible=False)
        fig3.update_layout(title="User transaction count",title_font=dict(size=33),title_font_color='#6739b7', height=500)
        st.plotly_chart(fig3,use_container_width=True)  
           
        #transaction bar chart
        map_user_qry_rslt1['State'] = map_user_qry_rslt1['State'].astype(str)
        # df_map_trans_query1['Transaction_count'] = df_map_trans_query1['Transaction_count'].astype(float)
        map_user_qry_rslt1_fig = px.bar(map_user_qry_rslt1, x = 'State', y ='Registerd_user', color ='Registerd_user', color_continuous_scale = 'thermal', title = 'Transaction User Analysis Chart', height = 700,)
        map_user_qry_rslt1_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
        st.plotly_chart(map_user_qry_rslt1_fig,use_container_width=True)


else:
        # Select tab
    tab1, tab2 ,tab3,tab4= st.tabs(['Top Transaction','Top District Transaction','Top User','Top district User'])   

    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            top_year = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='top_year')
        with col2:
            top_quatr = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='top_quatr')

        cursor.execute(f"SELECT State,District_pincode, Trans_amount FROM Top_transaction WHERE Year = '{ top_year}' AND Quater = '{top_quatr}';")
        top_trans_qry_rslt = cursor.fetchall()
        top_trans_qry_rslt = pd.DataFrame(np.array(top_trans_qry_rslt), columns=['State','District_pincode','Trans_amount'])
        top_trans_qry_rslt1 = top_trans_qry_rslt.set_index(pd.Index(range(1, len(top_trans_qry_rslt)+1)))    

        cursor.execute(f"SELECT State,SUM( Trans_amount),SUM(Trans_count) FROM Top_transaction WHERE Year = '{ top_year}' AND Quater = '{top_quatr}' GROUP BY State;")
        top_trans1_qry_rslt = cursor.fetchall()
        top_trans1_qry_rslt = pd.DataFrame(np.array(top_trans1_qry_rslt), columns=['State','Trans_amount_sum','Trans_count_sum'])
        top_trans1_qry_rslt1 = top_trans1_qry_rslt.set_index(pd.Index(range(1, len(top_trans1_qry_rslt)+1)))  
        #-------------------geo visualization----------------------------
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        data1 = json.loads(response.content)

        df =  top_trans_qry_rslt
        fig4 = px.choropleth(
            df,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='State',
            color='District_pincode',
            hover_name='Trans_amount',
            color_continuous_scale='Reds'
        )

        fig4.update_geos(fitbounds="locations", visible=False)
        fig4.update_layout(title="Top transaction count",title_font=dict(size=33),title_font_color='#6739b7', height=500)
        st.plotly_chart(fig4,use_container_width=True) 

        #top transaction bar ----------------------------------------

        top_trans1_qry_rslt1['State'] = top_trans1_qry_rslt1['State'].astype(str)
        top_trans1_qry_rslt1['Trans_amount_sum'] = top_trans1_qry_rslt1['Trans_amount_sum'].astype(float)
        top_trans1_qry_rslt1_fig = px.bar(top_trans1_qry_rslt1, x = 'State', y ='Trans_amount_sum', color ='Trans_amount_sum', color_continuous_scale = 'thermal', title = 'Top Transaction Analysis Chart', height = 700,)
        top_trans1_qry_rslt1_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
        st.plotly_chart(top_trans1_qry_rslt1_fig,use_container_width=True) 

        st.header(':violet[Total calculation]')
        st.subheader(':red[Top Transaction Analysis]')
        st.dataframe(top_trans1_qry_rslt1)

    with tab2:
        col1, col2 = st.columns(2)
        with col1:
            top1_year = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='top1_year')
        with col2:
            top1_quatr = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='top1_quatr')

        cursor.execute(f"SELECT State,District_name, Trans_amount FROM Top_trans_district WHERE Year = '{ top1_year}' AND Quater = '{top1_quatr}';")
        top_trans_distct_qry_rslt = cursor.fetchall()
        top_trans_distct_qry_rslt = pd.DataFrame(np.array(top_trans_distct_qry_rslt), columns=['State','District_name','Trans_amount'])
        top_trans_distct_qry_rslt1 = top_trans_distct_qry_rslt.set_index(pd.Index(range(1, len(top_trans_distct_qry_rslt)+1)))    

        cursor.execute(f"SELECT State,SUM( Trans_amount),SUM(Trans_count) FROM Top_trans_district WHERE Year = '{ top_year}' AND Quater = '{top_quatr}' GROUP BY State;")
        top_trans1_distct_qry_rslt = cursor.fetchall()
        top_trans1_distct_qry_rslt = pd.DataFrame(np.array(top_trans1_distct_qry_rslt), columns=['State','Trans_amount_sum','Trans_count_sum'])
        top_trans1_distct_qry_rslt1 = top_trans1_distct_qry_rslt.set_index(pd.Index(range(1, len(top_trans1_distct_qry_rslt)+1)))  
       #-------------------geo visualization--------------------
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        data1 = json.loads(response.content)

        df =  top_trans_distct_qry_rslt
        fig5= px.choropleth(
            df,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='State',
            color='District_name',
            hover_name='Trans_amount',
            color_continuous_scale='Reds'
        )

        fig5.update_geos(fitbounds="locations", visible=False)
        fig5.update_layout(title="Top transaction district count",title_font=dict(size=33),title_font_color='#6739b7', height=500)
        st.plotly_chart(fig5,use_container_width=True) 

        #top transaction bar ----------------------------------------

        top_trans1_qry_rslt1['State'] = top_trans1_qry_rslt1['State'].astype(str)
        top_trans1_qry_rslt1_fig = px.bar(top_trans1_qry_rslt1, x = 'State', y ='Trans_count_sum', color ='Trans_count_sum', color_continuous_scale = 'thermal', title = 'Top Transaction district Analysis Chart', height = 700,)
        top_trans1_qry_rslt1_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
        st.plotly_chart(top_trans1_qry_rslt1_fig,use_container_width=True) 

        st.header(':violet[Total calculation]')
        st.subheader(':red[Top Transaction Analysis]')
        st.dataframe(top_trans1_qry_rslt1) 

        #---------------top user ----------------------

    with tab3:
        col1, col2 = st.columns(2)
        with col1:
            top2_year = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='top2_year')
        with col2:
            top2_quatr = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='top2_quatr') 

        cursor.execute(f"SELECT State, District_pincode, Reg_user_count FROM Top_user WHERE Year = '{ top2_year}' AND Quater = '{top2_quatr}';")
        top_user_qry_rslt = cursor.fetchall()
        top_user_qry_rslt = pd.DataFrame(np.array(top_user_qry_rslt), columns=['State','District_pincode','Reg_user_count'])
        top_user_qry_rslt1 = top_user_qry_rslt.set_index(pd.Index(range(1, len(top_user_qry_rslt)+1))) 

        #-----------------------geo-visualization------------------------

        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        data1 = json.loads(response.content)

        df =  top_user_qry_rslt
        fig6= px.choropleth(
            df,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='State',
            color='District_pincode',
            hover_name='Reg_user_count',
            color_continuous_scale='Reds'
        )

        fig6.update_geos(fitbounds="locations", visible=False)
        fig6.update_layout(title="Top user count",title_font=dict(size=33),title_font_color='#6739b7', height=500)
        st.plotly_chart(fig6,use_container_width=True) 

        top_user_qry_rslt1['State'] = top_user_qry_rslt1['State'].astype(str)
        top_user_qry_rslt1_fig = px.bar(top_user_qry_rslt1, x = 'State', y ='Reg_user_count', color ='Reg_user_count', color_continuous_scale = 'thermal', title = 'Top user Analysis Chart', height = 700,)
        top_user_qry_rslt1_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
        st.plotly_chart(top_user_qry_rslt1_fig,use_container_width=True) 

        st.header(':violet[Total calculation]')
        st.subheader(':red[Top user Analysis]')
        st.dataframe(top_user_qry_rslt1)          
    
    #---------------------top user district------------------

    with tab4:
        col1, col2 = st.columns(2)
        with col1:
            top3_year = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023'),key='top3_year')
        with col2:
            top3_quatr = st.selectbox('**Select Quarter**', ('1','2','3','4'),key='top3_quatr') 

        cursor.execute(f"SELECT State, District_name, Reg_user_count FROM Top_user_district WHERE Year = '{ top3_year}' AND Quater = '{top3_quatr}';")
        top_user_district_qry_rslt = cursor.fetchall()
        top_user_district_qry_rslt = pd.DataFrame(np.array(top_user_district_qry_rslt), columns=['State','District_name','Reg_user_count'])
        top_user_district_qry_rslt1 = top_user_district_qry_rslt.set_index(pd.Index(range(1, len(top_user_district_qry_rslt)+1))) 

        #-----------------------geo-visualization------------------------

        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        data1 = json.loads(response.content)

        df =  top_user_district_qry_rslt
        fig7= px.choropleth(
            df,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='State',
            color='District_name',
            hover_name='Reg_user_count',
            color_continuous_scale='Reds'
        )

        fig7.update_geos(fitbounds="locations", visible=False)
        fig7.update_layout(title="Top user district count",title_font=dict(size=33),title_font_color='#6739b7', height=500)
        st.plotly_chart(fig7,use_container_width=True) 

               #---------------bar chart-----------------

        top_user_district_qry_rslt1['State'] = top_user_district_qry_rslt1['State'].astype(str)
        top_user_district_qry_rslt1_fig = px.bar(top_user_district_qry_rslt1, x = 'State', y ='Reg_user_count', color ='Reg_user_count', color_continuous_scale = 'thermal', title = 'Top user district Analysis Chart', height = 700,)
        top_user_district_qry_rslt1_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
        st.plotly_chart(top_user_district_qry_rslt1_fig,use_container_width=True) 

        st.header(':violet[Total calculation]')
        st.subheader(':red[Top user district Analysis]')
        st.dataframe(top_user_district_qry_rslt1)          
             
              
          


            



    