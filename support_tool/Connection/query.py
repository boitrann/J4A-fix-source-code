# -*- coding: utf-8 -*-
"""
Created on Thu Jan 21 05:47:11 2021

@author: Guest_1
"""
import sys
sys.path.insert(
	0, 'D:\Vendor_data_collect')

#from support_tool.Connection import Connect_y4abii as connection
import pandas as pd
import datetime


#def drop_table(ora_table):
#    cnn, cursor = connection.connection()
#    cursor.execute('drop table ' + ora_table)
    

#def query_table(sql):
#    cnn, cursor = connection.connection()
#    df = pd.read_sql_query(sql,cnn)    
#    cnn = None

#    return df



def run_sql(connection,sql):
    print(sql)
    cnn, cursor = connection.connection()
    cursor.execute(sql)
    cnn.commit()
    cnn = None
import  numpy as np
def insert_table(biicnn,df_or,table,incr_col):
    df=df_or.drop([incr_col],axis=1)
    df = df.replace({np.NAN: None})
    cnn, cursor = biicnn.connection()
    rows = [tuple(x) for x in df.values]
    NUM_COLS = len(df.columns)
    bind = ','.join([':' + str(i) for i in range(0, NUM_COLS)])
    cursor.executemany(
        'INSERT INTO {0} VALUES({1})'.format(table, bind), rows)
    cnn.commit()
    print(
        f'Insert {str(len(rows))} rows to {table} database successfully!')



def read_sql(sql,cnn):
    df=pd.read_sql_query(sql,con=cnn)
    return df
def run_sql(connection,sql):
    print(sql)
    cnn, cursor = connection.connection()
    cursor.execute(sql)
    cnn.commit()
    cnn = None
    
def query(connection,sql):
    cnn, cursor = connection.connection()
    # sql="SELECT * FROM SYSTEM.G_AVC_IVTR_REQ_COLLECT where STATUS ='Downloaded' "
    df = pd.read_sql_query(sql,cnn)
    # # df=cursor.execute(sql)
    # df=read_sql(sql,cnn)
    cnn = None

    return df    


def df_to_db(connection,df_new, exclusive_cols, db_name, table_name, announce):
    from support_tool.Connection import query as query  
    db_type = connection.db_type()
    if db_type == 'mysql':
        query.df_to_mysqltable(connection, df_new, exclusive_cols,db_name, table_name, announce)
    if db_type == 'oracle':
        if db_name == '' or db_name == None:
            query.df_to_oratable(connection, df_new, exclusive_cols, table_name, announce)
        else:
            query.df_to_oratable_2(connection, df_new, exclusive_cols, db_name,table_name, announce)

def df_to_oratable(connection,df_new, exclusive_cols, ora_table, announce):
    # select data type from ora table
    # print(ora_table)
    sql_select = "SELECT  table_name, column_name, data_type FROM all_tab_columns where table_name = '" + ora_table + "'"
    cnn, cursor = connection.connection()
    df_cols = pd.read_sql_query(sql_select,cnn)    
    cnn = None
    sql_select = None
    #print(df_cols['COLUMN_NAME'])
    #print(df_new.columns)
    # define 4 LIST: text, datime, int, float
    text_type_def = ['VARCHAR2', 'NVARCHAR2']
    datetime_type_def = ['TIMESTAMP(6)']
    int_type_def = ['NUMBER']
    float_type_def = ['FLOAT']
    
    text_cols = []
    date_time_cols = []
    int_cols = []
    float_cols = []
    data_input_cols = df_new.columns
    data_output_cols = []
    
    for index, row in df_cols.iterrows():
        col_name = row['COLUMN_NAME']
        col_type = row['DATA_TYPE']
        if col_name not in exclusive_cols and col_name in data_input_cols:
            data_output_cols = data_output_cols + [col_name]
            if col_type in text_type_def:
                text_cols = text_cols + [col_name]
            elif col_type in datetime_type_def:
                date_time_cols = date_time_cols + [col_name]
            elif col_type in int_type_def:
                int_cols = int_cols + [col_name]         
            elif col_type in float_type_def:
                float_cols = float_cols + [col_name]                  
    
    # DEFINE DF DATA FROM DF NEW WITH THESE COLUMNS OF 4 TYPES
    df_data = pd.DataFrame([], columns = data_output_cols)
    df_data = df_data.append(df_new[data_output_cols], ignore_index = True)
    #print(data_output_cols)
    # CONVERT DATA BEFORE IMPORT
    # Convert Datetime cols
    df_data[date_time_cols] = df_data[date_time_cols].astype('datetime64[ns]')
    
    for col in date_time_cols:
        df_data[col] = df_data[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_data[date_time_cols] = df_data[date_time_cols].astype(str)
    df_data[date_time_cols] = df_data[date_time_cols].replace({'nan': None})
    
    
    # Convert Text cols
    #text_cols = [ 'TYPE_1','TYPE_2', 'STATUS', 'COLLECTED_FILE_NAME', 'CONVERTED_FILE_NAME',  'REMARKS']
    df_data[text_cols] = df_data[text_cols].astype(str)
    df_data[text_cols] = df_data[text_cols].replace({'nan': None})
    df_data[text_cols] = df_data[text_cols].replace({'None': None})
    
    # Convert Int cols
    for col in int_cols:
        df_data[col] = pd.to_numeric(df_data[col] , downcast='signed')
    df_data[int_cols] = df_data[int_cols].fillna(0)
    
    # Convert Float cols
    for col in float_cols:
        df_data[col] = pd.to_numeric(df_data[col] , downcast='float')
    df_data[float_cols] = df_data[float_cols].fillna(0)
    
    
    df_data = df_data.where(pd.notnull(df_data), None)
    

    
    ora_cols_text = ','.join(data_output_cols)
    count = 0
    ora_values_list = []
    for col in data_output_cols:
        count = count +1    
        if col in   date_time_cols:
            value = "TO_DATE(:" + str(count) + ",'YYYY-MM-DD HH24:MI:SS')"
        else:
            value = ":" + str(count)
        ora_values_list = ora_values_list + [value]
    ora_values_text = ','.join(ora_values_list)
    sql_end = """ (""" + ora_cols_text + """) values (""" + ora_values_text + """)"""
    
    # RUN SQL INPORT
    df_data = df_data[data_output_cols]
    list_data = df_data.values.tolist()
    if announce == True:
        print(f'{datetime.datetime.now()} - Import table {ora_table}')
    sql_insert = 'insert into ' + ora_table + sql_end
    #print(sql_insert)
    cnn, cursor = connection.connection()
    print(sql_insert)
    cursor.executemany(sql_insert, list_data)
    cnn.commit()
    cnn = None
    if announce == True:
        print(f'{datetime.datetime.now()} - Done')
    list_data = None
    sql_insert = None    
    

def df_to_mysqltable(connection,df_new, exclusive_cols, db_name, table_name, announce):
    # select data type from ora table
    sql_select = ("""SELECT TABLE_NAME, COLUMN_NAME , DATA_TYPE
                      FROM INFORMATION_SCHEMA.COLUMNS
                      WHERE TABLE_SCHEMA = '""" + db_name + """' and table_name = '""" + table_name + "'""")
    cnn, cursor = connection.connection()
    df_cols = pd.read_sql_query(sql_select,cnn)    
    cnn = None
    sql_select = None
    
    # define 4 LIST: text, datime, int, float
    text_type_def = ['varchar', 'nvarchar']
    datetime_type_def = ['datetime']
    int_type_def = ['int']
    float_type_def = ['double','float','decimal']
    
    text_cols = []
    date_time_cols = []
    int_cols = []
    float_cols = []
    data_input_cols = df_new.columns
    data_output_cols = []
    
    for index, row in df_cols.iterrows():
        col_name = row['COLUMN_NAME']
        col_type = row['DATA_TYPE']
        if col_name not in exclusive_cols and col_name in data_input_cols:
            data_output_cols = data_output_cols + [col_name]
            if col_type in text_type_def:
                text_cols = text_cols + [col_name]
            elif col_type in datetime_type_def:
                date_time_cols = date_time_cols + [col_name]
            elif col_type in int_type_def:
                int_cols = int_cols + [col_name]         
            elif col_type in float_type_def:
                float_cols = float_cols + [col_name]                  
    
    # DEFINE DF DATA FROM DF NEW WITH THESE COLUMNS OF 4 TYPES
    df_data = pd.DataFrame([], columns = data_output_cols)
    df_data = df_data.append(df_new[data_output_cols], ignore_index = True)
    
    # CONVERT DATA BEFORE IMPORT
    # Convert Datetime cols
    df_data[date_time_cols] = df_data[date_time_cols].astype('datetime64[ns]')
    
    for col in date_time_cols:
        df_data[col] = df_data[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_data[date_time_cols] = df_data[date_time_cols].astype(str)
    df_data[date_time_cols] = df_data[date_time_cols].replace({'nan': None})
    
    
    # Convert Text cols
    #text_cols = [ 'TYPE_1','TYPE_2', 'STATUS', 'COLLECTED_FILE_NAME', 'CONVERTED_FILE_NAME',  'REMARKS']
    df_data[text_cols] = df_data[text_cols].astype(str)
    df_data[text_cols] = df_data[text_cols].replace({'nan': None})
    df_data[text_cols] = df_data[text_cols].replace({'None': None})
    
    # Convert Int cols
    for col in int_cols:
        df_data[col] = pd.to_numeric(df_data[col] , downcast='signed')
    df_data[int_cols] = df_data[int_cols].fillna(0)
    
    # Convert Float cols
    for col in float_cols:
        df_data[col] = pd.to_numeric(df_data[col] , downcast='float')
    df_data[float_cols] = df_data[float_cols].fillna(0)
    
    
    df_data = df_data.where(pd.notnull(df_data), None)
    

    
    ora_cols_text = ','.join(data_output_cols)
    count = 0
    ora_values_list = []
    for col in data_output_cols:
        count = count +1    
        value = "%s"
        ora_values_list = ora_values_list + [value]
    ora_values_text = ','.join(ora_values_list)
    sql_end = """ (""" + ora_cols_text + """) values (""" + ora_values_text + """)"""
    
    # RUN SQL INPORT
    df_data = df_data[data_output_cols]
    list_data = df_data.values.tolist()
    tpls = [tuple(x) for x in df_data.to_numpy()]
    if announce == True:
        print(f'{datetime.datetime.now()} - Import table {table_name}')
    sql_insert = 'insert into ' + db_name + '.' + table_name + sql_end
    cnn, cursor = connection.connection()
    cursor.executemany(sql_insert, tpls)
    cnn.commit()
    cnn = None
    if announce == True:
        print(f'{datetime.datetime.now()} - Done')
    list_data = None
    sql_insert = None            


def json_to_oratable(connection,json, exclusive_cols, ora_table, announce):  
    df = pd.DataFrame([], columns = [])
        
    from support_tool.Connection import query as query  
    
    df = df.append(json, ignore_index = True)
    
    query.df_to_oratable(connection, df, exclusive_cols, ora_table, announce)


def json_to_oratable_2(connection,json, exclusive_cols, owner, ora_table, announce):  
    df = pd.DataFrame([], columns = [])
        
    from support_tool.Connection import query as query  
    
    df = df.append(json, ignore_index = True)
    
    query.df_to_oratable_2(connection, df, exclusive_cols, owner,ora_table, announce)    

def json_to_mysqltable(connection,json, exclusive_cols,db_name, ora_table, announce):  
    df = pd.DataFrame([], columns = [])
        
    from support_tool.Connection import query as query  
    
    df = df.append(json, ignore_index = True)
  
    query.df_to_mysqltable(connection, df, exclusive_cols,db_name, ora_table, announce)


def json_to_db(connection,json, exclusive_cols, db_name, table_name, announce):
    from support_tool.Connection import query as query  
    db_type = connection.db_type() # 'Oracle'
    if db_type == 'mysql':
        query.json_to_mysqltable(connection,json, exclusive_cols,db_name, table_name, announce)
    if db_type == 'oracle':
        if db_name == None or db_name == '':
            query.json_to_oratable(connection, json, exclusive_cols, table_name, announce)
        else:
            query.json_to_oratable_2(connection, json, exclusive_cols,db_name, table_name, announce)


def json_update_to_db(connection, json, update_cols, condition_cols,db_name, db_table):
    from support_tool.Connection import query as query  
    db_type = connection.db_type()
    # if db_type == 'mysql':
    #     query.json_update_to_mysqldb(connection, json, update_cols, condition_cols, db_name, db_table)
    if db_type == 'oracle':
        if db_name == None or db_name == '':
            query.json_update_to_oradb(connection, json, update_cols, condition_cols, db_table)
        else:
            query.json_update_to_oradb_2(connection, json, update_cols, condition_cols, db_name, db_table)

    
    
def json_update_to_oradb(connection, json, update_cols, condition_cols, db_table):     
    from support_tool.Connection import query as query
    update_status = False
    # select data type from ora table
    df_cols = query.query(connection, "SELECT  table_name, column_name, data_type FROM all_tab_columns where table_name = '" + db_table + "'")

    nonmatch_cols = []
    # define 4 LIST: text, datime, int, float
    text_type_def = ['VARCHAR2', 'NVARCHAR2']
    datetime_type_def = ['TIMESTAMP(6)']
    int_type_def = ['NUMBER']
    float_type_def = ['FLOAT']
    
    text_cols = []
    date_time_cols = []
    int_cols = []
    float_cols = []
    
    data_input_cols = []
    for key, value in json.items():
        data_input_cols = data_input_cols +[key]
    #data_input_cols = df_new.columns
    matched_cols = []
    
    # merge to get same columns in table and json
    for index, row in df_cols.iterrows():
        col_name = row['COLUMN_NAME']
        col_type = row['DATA_TYPE']
        if col_name in data_input_cols:
            matched_cols = matched_cols + [col_name]
            if col_type in text_type_def:
                text_cols = text_cols + [col_name]
            elif col_type in datetime_type_def:
                date_time_cols = date_time_cols + [col_name]
            elif col_type in int_type_def:
                int_cols = int_cols + [col_name]         
            elif col_type in float_type_def:
                float_cols = float_cols + [col_name]                  
    
    
    # check match update col:
    match_update_col = True
    for update_col in update_cols:
        if update_col not in matched_cols:
            nonmatch_cols = nonmatch_cols + [update_col]
            match_update_col = False
    
    # check match condition col:
    match_condition_col = True
    for condition_col in condition_cols:
        if condition_col not in matched_cols:
            nonmatch_cols = nonmatch_cols + [condition_col]
            match_condition_col = False        
    
    if match_update_col == False or match_condition_col == False:
        print(f'Error: Condition Columns or Update Columns not in table columns {str(nonmatch_cols)}')
        #'a'.get()
    else:
        # Tạo query update
        update_sql_list = []
        for col in update_cols: 
            update_sql = col + ' '
            if json[col] == None:
                update_sql = update_sql + ' = Null '
            elif col in   date_time_cols:
                update_sql = update_sql + ' = '+ " TO_DATE('" + json[col].strftime('%Y-%m-%d %H:%M:%S') + "','YYYY-MM-DD HH24:MI:SS')"
            elif col in text_cols:
                update_sql = update_sql + ' = '+ "'" + str(json[col]).replace("'",'"') + "'"
            else:
                update_sql = update_sql + ' = '+ str(json[col])
            update_sql_list = update_sql_list + [update_sql]
        update_sql_text = ','.join(update_sql_list)
        # Tạo query condition
        condition_sql_list = []
        for col in condition_cols: 
            sql = col + ' '
            if json[col] == None:
                sql = sql + ' is null '
            elif col in   date_time_cols:
                sql = sql + ' = ' + " TO_DATE('" + json[col].strftime('%Y-%m-%d %H:%M:%S') + "','YYYY-MM-DD HH24:MI:SS')"
            elif col in text_cols:
                sql = sql + ' = '+ "'" + str(json[col]).replace("'",'"') + "'"
            else:
                sql = sql + ' = '+ str(json[col])
            condition_sql_list = condition_sql_list + [sql]
        condition_sql_text = ' and '.join(condition_sql_list)
        
        sql_update = (""" update """ + db_table + """ set """ + update_sql_text + """ where """ + condition_sql_text)
        #print()
        #print(sql_update)
        query.run_sql(connection,sql_update )
        update_status = True
        #print(update_status)
        #print()
    return update_status    
    
    
def json_update_to_oradb_2(connection, json, update_cols, condition_cols, owner, db_table):     
    from support_tool.Connection import query as query
    update_status = False
    # select data type from ora table
    df_cols = query.query(connection, """SELECT  table_name, column_name, data_type 
                          FROM all_tab_columns 
                          where table_name = '""" + db_table.upper() + "' and owner = '" + owner.upper() + "'")

    nonmatch_cols = []
    # define 4 LIST: text, datime, int, float
    text_type_def = ['VARCHAR2', 'NVARCHAR2']
    datetime_type_def = ['TIMESTAMP(6)']
    int_type_def = ['NUMBER']
    float_type_def = ['FLOAT']
    
    text_cols = []
    date_time_cols = []
    int_cols = []
    float_cols = []
    
    data_input_cols = []
    for key, value in json.items():
        data_input_cols = data_input_cols +[key]
    #data_input_cols = df_new.columns
    matched_cols = []
    
    # merge to get same columns in table and json
    for index, row in df_cols.iterrows():
        col_name = row['COLUMN_NAME']
        col_type = row['DATA_TYPE']
        if col_name in data_input_cols:
            matched_cols = matched_cols + [col_name]
            if col_type in text_type_def:
                text_cols = text_cols + [col_name]
            elif col_type in datetime_type_def:
                date_time_cols = date_time_cols + [col_name]
            elif col_type in int_type_def:
                int_cols = int_cols + [col_name]         
            elif col_type in float_type_def:
                float_cols = float_cols + [col_name]                  
    
    
    # check match update col:
    match_update_col = True
    for update_col in update_cols:
        if update_col not in matched_cols:
            nonmatch_cols = nonmatch_cols + [update_col]
            match_update_col = False
    
    # check match condition col:
    match_condition_col = True
    for condition_col in condition_cols:
        if condition_col not in matched_cols:
            nonmatch_cols = nonmatch_cols + [condition_col]
            match_condition_col = False        
    
    if match_update_col == False or match_condition_col == False:
        print(f'Error: Condition Columns or Update Columns not in table columns {str(nonmatch_cols)}')
        #'a'.get()
    else:
        # Tạo query update
        update_sql_list = []
        for col in update_cols: 
            update_sql = col + ' '
            if json[col] == None:
                update_sql = update_sql + ' = Null '
            elif col in   date_time_cols:
                update_sql = update_sql + ' = '+ " TO_DATE('" + json[col].strftime('%Y-%m-%d %H:%M:%S') + "','YYYY-MM-DD HH24:MI:SS')"
            elif col in text_cols:
                update_sql = update_sql + ' = '+ "'" + str(json[col]).replace("'",'"') + "'"
            else:
                update_sql = update_sql + ' = '+ str(json[col])
            update_sql_list = update_sql_list + [update_sql]
        update_sql_text = ','.join(update_sql_list)
        # Tạo query condition
        condition_sql_list = []
        for col in condition_cols: 
            sql = col + ' '
            if json[col] == None:
                sql = sql + ' is null '
            elif col in   date_time_cols:
                sql = sql + ' = ' + " TO_DATE('" + json[col].strftime('%Y-%m-%d %H:%M:%S') + "','YYYY-MM-DD HH24:MI:SS')"
            elif col in text_cols:
                sql = sql + ' = '+ "'" + str(json[col]).replace("'",'"') + "'"
            else:
                sql = sql + ' = '+ str(json[col])
            condition_sql_list = condition_sql_list + [sql]
        condition_sql_text = ' and '.join(condition_sql_list)
        
        sql_update = (""" update """ + owner + '.' +db_table + """ set """ + update_sql_text + """ where """ + condition_sql_text)
        #print()
        #print(sql_update)
        query.run_sql(connection,sql_update )
        update_status = True
        #print(update_status)
        #print()
    return update_status    
    
    
def json_update_to_mysqldb(connection, json, update_cols, condition_cols, db_name, db_table):     
    
    from support_tool.Connection import query as query
    df_cols = query.query(connection, """SELECT TABLE_NAME, COLUMN_NAME , DATA_TYPE
                      FROM INFORMATION_SCHEMA.COLUMNS
                      WHERE TABLE_SCHEMA = '""" + db_name + """' and table_name = '""" + db_table + "'""")

    nonmatch_cols = []
    # define 4 LIST: text, datime, int, float
    text_type_def = ['varchar', 'nvarchar']
    datetime_type_def = ['datetime']
    int_type_def = ['int']
    float_type_def = ['double']
    
    text_cols = []
    date_time_cols = []
    int_cols = []
    float_cols = []
    
    data_input_cols = []
    for key, value in json.items():
        data_input_cols = data_input_cols +[key]
    #data_input_cols = df_new.columns
    matched_cols = []
    
    # merge to get same columns in table and json
    for index, row in df_cols.iterrows():
        col_name = row['COLUMN_NAME']
        col_type = row['DATA_TYPE']
        if col_name in data_input_cols:
            matched_cols = matched_cols + [col_name]
            if col_type in text_type_def:
                text_cols = text_cols + [col_name]
            elif col_type in datetime_type_def:
                date_time_cols = date_time_cols + [col_name]
            elif col_type in int_type_def:
                int_cols = int_cols + [col_name]         
            elif col_type in float_type_def:
                float_cols = float_cols + [col_name]                  
    
    
    # check match update col:
    match_update_col = True
    for update_col in update_cols:
        if update_col not in matched_cols:
            nonmatch_cols = nonmatch_cols + [update_col]
            match_update_col = False
    
    # check match condition col:
    match_condition_col = True
    for condition_col in condition_cols:
        if condition_col not in matched_cols:
            nonmatch_cols = nonmatch_cols + [condition_col]
            match_condition_col = False        
    
    if match_update_col == False or match_condition_col == False:
        print(f'Error: Condition Columns or Update Columns not in table columns {str(nonmatch_cols)}')
        #'a'.get()
    else:
        # Tạo query update
        update_sql_list = []
        for col in update_cols: 
            update_sql = col + ' '
            if json[col] == None:
                update_sql = update_sql + ' = Null '
            elif col in   date_time_cols:
                update_sql = update_sql + ' = '+ " STR_TO_DATE('" + json[col].strftime('%Y-%m-%d %H:%M:%S') + "','%Y-%m-%d %h:%i:%s')"
            elif col in text_cols:
                update_sql = update_sql + ' = '+ "'" + str(json[col]).replace("'",'"') + "'"
            else:
                update_sql = update_sql + ' = '+ str(json[col])
            update_sql_list = update_sql_list + [update_sql]
        update_sql_text = ','.join(update_sql_list)
        # Tạo query condition
        condition_sql_list = []
        for col in condition_cols: 
            sql = col + ' '
            if json[col] == None:
                sql = sql + ' is null '
            elif col in   date_time_cols:
                sql = sql + ' = ' + " STR_TO_DATE('" + json[col].strftime('%Y-%m-%d %H:%M:%S') + "','%Y-%m-%d %h:%i:%s')"
            elif col in text_cols:
                sql = sql + ' = '+ "'" + str(json[col]).replace("'",'"') + "'"
            else:
                sql = sql + ' = '+ str(json[col])
            condition_sql_list = condition_sql_list + [sql]
        condition_sql_text = ' and '.join(condition_sql_list)
        
        sql_update = (""" update """ +db_name+'.'+ db_table + """ set """ + update_sql_text + """ where """ + condition_sql_text)
        #print()
        #print(sql_update)
        query.run_sql(connection,sql_update )
        update_status = True
        #print(update_status)
        #print()
    return update_status  
    

def df_to_oratable_2(connection,df_new, exclusive_cols, owner, ora_table, announce):
    """

    :param connection: Connection string
    :param df_new: dataframe - data insert
    :param exclusive_cols: list -  not insert data of column in list
    :param owner: schema
    :param ora_table: table name
    :param announce: Print result if announce = 'True'
    :return:
    """
    ### Lấy column name và data type của tab
    sql_select = """SELECT  table_name, column_name, data_type FROM all_tab_columns 
                    where table_name = '""" + ora_table + "' and owner = '" + owner.upper() + "'"
    cnn, cursor = connection.connection()
    df_cols = pd.read_sql_query(sql_select,cnn)    
    cnn = None
    sql_select = None

    # define 4 LIST: text, datime, int, float
    text_type_def = ['VARCHAR2', 'NVARCHAR2']
    datetime_type_def = ['TIMESTAMP(6)']
    int_type_def = ['NUMBER']
    float_type_def = ['FLOAT']
    
    text_cols = []
    date_time_cols = []
    int_cols = []
    float_cols = []
    data_input_cols = df_new.columns
    data_output_cols = []

    ### Phân data type ứng với các cột
    for index, row in df_cols.iterrows():
        col_name = row['COLUMN_NAME']
        col_type = row['DATA_TYPE']
        if col_name not in exclusive_cols and col_name in data_input_cols:
            data_output_cols = data_output_cols + [col_name]
            if col_type in text_type_def:
                text_cols = text_cols + [col_name]
            elif col_type in datetime_type_def:
                date_time_cols = date_time_cols + [col_name]
            elif col_type in int_type_def:
                int_cols = int_cols + [col_name]         
            elif col_type in float_type_def:
                float_cols = float_cols + [col_name]                  
    
    # DEFINE DF DATA FROM DF NEW WITH THESE COLUMNS OF 4 TYPES
    df_data = pd.DataFrame([], columns = data_output_cols)
    df_data = df_data.append(df_new[data_output_cols], ignore_index = True)


    # CONVERT DATA BEFORE IMPORT
    # Convert Datetime cols
    df_data[date_time_cols] = df_data[date_time_cols].astype('datetime64[ns]')
    
    for col in date_time_cols:
        df_data[col] = df_data[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_data[date_time_cols] = df_data[date_time_cols].astype(str)
    df_data[date_time_cols] = df_data[date_time_cols].replace({'nan': None})
    
    
    # Convert Text cols
    df_data[text_cols] = df_data[text_cols].astype(str)
    df_data[text_cols] = df_data[text_cols].replace({'nan': None})
    df_data[text_cols] = df_data[text_cols].replace({'None': None})
    
    # Convert Int cols
    for col in int_cols:
        df_data[col] = pd.to_numeric(df_data[col] , downcast='signed')
    df_data[int_cols] = df_data[int_cols].fillna(0)
    
    # Convert Float cols
    for col in float_cols:
        df_data[col] = pd.to_numeric(df_data[col] , downcast='float')
    df_data[float_cols] = df_data[float_cols].fillna(0)
    


    df_data = df_data.where(pd.notnull(df_data), None)
    ora_cols_text = ','.join(data_output_cols)
    count = 0
    ora_values_list = []
    for col in data_output_cols:
        count = count +1    
        if col in   date_time_cols:
            value = "TO_DATE(:" + str(count) + ",'YYYY-MM-DD HH24:MI:SS')"
        else:
            value = ":" + str(count)
        ora_values_list = ora_values_list + [value]
    ora_values_text = ','.join(ora_values_list)
    sql_end = """ (""" + ora_cols_text + """) values (""" + ora_values_text + """)"""
    
    # RUN SQL INPORT
    df_data = df_data[data_output_cols]
    list_data = df_data.values.tolist()
    # print('step1')
    if announce == True:
        print(f'{datetime.datetime.now()} - Import table {ora_table}')
    sql_insert = 'insert into ' + owner + '.' + ora_table + sql_end
    #print(sql_insert)
    cnn, cursor = connection.connection()
    try:
        cursor.executemany(sql_insert, list_data)
    except EOFError as e:
        print(e)

    cnn.commit()
    cnn = None
    if announce == True:
        print(f'{datetime.datetime.now()} - Done')
    list_data = None
    sql_insert = None    
    
    