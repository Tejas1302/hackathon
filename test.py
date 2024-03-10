# Import python packages
from snowflake.snowpark import Session
import pandas as pd
import requests
import snowflake.connector
from urllib.error import URLError
import streamlit as st
from snowflake.snowpark.context import get_active_session

# if 'layout_preference' not in st.session_state:
#     st.session_state['layout_preference'] = 'wide'
#     st.set_page_config(page_title="Web Application", page_icon=":bulb:", layout=st.session_state['layout_preference'])

def create_session():
    return Session.builder.configs(st.secrets["snowflake"]).create()

session = create_session()

# Write directly to the app
st.title("Welcome to Streamlit in Snowflake")
st.header("Saama Thunder's") 
status = st.radio("Select One of the Stage: ", ('Internal Satge', 'External Stage(S3)','Named Stage'))

# conditional statement to print 
if (status == 'Internal Satge'):
    st.success("You have selected Internal Stage")
elif (status=='External Stage(S3)'):
    st.success("You have selected External Stage(S3)")
elif(status=='Named Stage'):
    st.success("You have selected Named Stage")
else:
    st.info("You haven't selected anything")

#con=st.connection("snowflake")
#df=con.query("select 1 from dual")
if (status == 'Internal Satge'):
#   session = get_active_session()
    st.subheader("There are no files in Internal Stage")

if (status == 'Named Stage'):
#   session = get_active_session()
    st.subheader("There are no files in Named Stage")

if(status=='External Stage(S3)'):
    #session = get_active_session()

    st.subheader("External Stage Files:")
    
    sql = "LIST @STORE_DB.ATLAS.AWS_S3_STG;"
    
    df = session.sql(sql).collect()
    
    st.dataframe(data=df,use_container_width=True)

    #from pathlib import Path

    #file_path = df[0]['name']
    #st.success(Path(file_path).name)
    
    #files = st.selectbox("Please select any one file from the below list",(df))
    
    sql_filename = "select distinct METADATA$FILENAME from @STORE_DB.ATLAS.AWS_S3_STG"

    df_filename = session.sql(sql_filename).collect()

    files = st.selectbox("Please select any one file from the below list",(df_filename))
    #st.write("You select",files)
    
    #st.button("Preview",type="primary")
    #Write directly to the app
    if 'clicked' not in st.session_state:
        st.session_state.clicked = False

    def click_button():
        st.session_state.clicked = True

    st.button('PREVIEW', on_click=click_button, type = 'primary')

    #Preview code
    if st.session_state.clicked:
        # The message and nested widget will remain on the page
        st.write("You have selected",files)
        #extract the table name from file name
        table_name = files.replace(' ', '')[:-4].upper()

        #sql_pull=f" SELECT $1,$2,$3,$4,$5,$6,$7,$8 FROM @AWS_S3_STG/{files};"
        sql_truncate = f"TRUNCATE TABLE STORE_DB.ATLAS.{table_name};"
        session.sql(sql_truncate).collect()
        sql_copy = f"COPY INTO STORE_DB.ATLAS.{table_name} FROM @STORE_DB.ATLAS.AWS_S3_STG/{files} FILE_FORMAT=F1"
        session.sql(sql_copy).collect()
        sql_select = f"select * from STORE_DB.ATLAS.{table_name} limit 10"
        df_sql_select = session.sql(sql_select).collect()
        #st.write(df_sql_select)
        dataframe_select = st.dataframe(df_sql_select)

        #Column Names
        sql_columns = f"select column_name from information_schema.columns where table_name = '{table_name}' and table_schema = 'ATLAS';"
        df_sql_columns = session.sql(sql_columns).collect()
        df_columns = pd.DataFrame(df_sql_columns)

        #For practice purpose
        st.markdown('----------------------')

        
        #Multiselect
        st.subheader("Data Profiling")
        st.caption('Please select any one column')
        #@st.cache
        #column_names = st.multiselect('Please select any column:',df_sql_columns)
        #st.write("Selected Columns : ",column_names)
#         if 'Onclicked' not in st.session_state:
#             st.session_state.Onclicked = False

        #Checkbox
        #Function to create checkbox
        def checkbox_container(data):
            #select_column_box = st.text_input('Please select any column')
            cols = st.columns(5)
            if cols[0].button('Select All', type = 'primary'):
                for i in data['COLUMN_NAME']:
                    st.session_state['dynamic_checkbox_' + i] = True
                st.experimental_rerun()
            if cols[1].button('UnSelect All',type='primary'):
                for i in data['COLUMN_NAME']:
                    st.session_state['dynamic_checkbox_' + i] = False
                st.experimental_rerun()
            for i in data['COLUMN_NAME']:
                	st.checkbox(i, key='dynamic_checkbox_' + i)

        #Function to print selected columns    
        def get_selected_checkboxes():
            return [i.replace('dynamic_checkbox_','') for i in st.session_state.keys() if i.startswith('dynamic_checkbox_') and st.session_state[i]]

        checkbox_container(df_columns)
        new_data = st.text_input('You selected',get_selected_checkboxes())
        #st.write(get_selected_checkboxes())

        selected_column = get_selected_checkboxes()

        for values in selected_column:
            #sql query to preview the data for selected columns
            # to show the distinct records
            select_sql = f"select distinct {values} from {table_name}"
            collect_select_sql = session.sql(select_sql).collect()
            df_select_sql = pd.DataFrame(collect_select_sql)
            st.write(df_select_sql)

        col1, col2 = st.columns(2)

        with col1:
            st.header("Distinct Record")
            select_sql = f"select distinct {values} from {table_name}"
            collect_select_sql = session.sql(select_sql).collect()
            df_select_sql = pd.DataFrame(collect_select_sql)
            st.write(df_select_sql)

        with col2:
            st.header("Counts")
            count_sql = f"select {values},count(*) as Total from {table_name} group by {values}"
            collect_count_sql = session.sql(count_sql).collect()
            df_count_sql = pd.DataFrame(collect_count_sql)
            st.write(df_count_sql)
            

 
# Tejas      
# # 
#         def click_button1():
#             st.session_state.Onclicked = True
# # 
#         st.button('SUBMIT', on_click=click_button1, type = 'primary')
#         if st.session_state.Onclicked:
#             st.write("Selected Columns : ",column_names)

#st.session_state.clicked = False 
