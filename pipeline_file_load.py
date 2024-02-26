import snowflake.connector
import pandas as pd
from datetime import date
from snowflake.connector.pandas_tools import write_pandas

con = snowflake.connector.connect(
    user = <<username>>,
    password = <<password>>,
    account = <<accountname>>
)

cs = con.cursor()

try:

    #FETCH FILE DETAILS FROM DATABASE
    cs.execute('use database ETL')

    get_file_info = 'select id,file_path,filename,file_type,delimited,load_date from etl.file_load.file_info where ACTIVE = \'Y\' '
    cs.execute(get_file_info)
    details = cs.fetchall()
    print(len(details))
    
    for file_count in range (0,len(details)):
        file_path,file_name,file_type,delimited,load_date = details[file_count][1],details[file_count][2],details[file_count][3],details[file_count][4],details[file_count][5]
        if load_date == date.today() :
            data = pd.read_table(file_path+file_name+file_type, delimiter=delimited)
    
            success, nchunks, nrows, _ = write_pandas(conn=con
                                          ,df=data
                                          ,table_name=file_name
                                          ,database='ETL'
                                          ,schema='FILE_LOAD'
                                          ,auto_create_table=True)
            
            insert_str = 'insert into etl.file_load.file_load_info values(\'{}\',\'{}\',{})'

            if success:
                loaded = 'SUCCESS'
            else:
                loaded = 'FAILED'
                
            cs.execute(insert_str.format(file_name,loaded,str(nrows)))

finally:
    cs.close()
