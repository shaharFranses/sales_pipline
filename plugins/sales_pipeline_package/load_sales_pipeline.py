import pandas as pd 
import sqlalchemy as sa
import os 


def run_sales_elt():
    DB_CONN_STRING = os.getenv('DB_CONN_STRING','postgresql://airflow:airflow@postgres:5432/airflow')
    SALES_FILE_PATH = os.getenv('SALES_FILE_PATH','sales_data.csv')

    engine = sa.create_engine(DB_CONN_STRING)
    #first step E- extraction ,take data from csv and turn it into dataframe
    sales_df=pd.read_csv(SALES_FILE_PATH)

    ##data integrityi checks

    bad_rows_mask=(
    sales_df['sale_id'].isnull()|
    sales_df['transaction_date'].isnull()|
    sales_df['amount'].isnull()|
    sales_df['amount']<0)
            
    
    DLQ_rows=sales_df[bad_rows_mask]
    sales_df=sales_df[~bad_rows_mask]

    #second step L- load , take dataframe and load it into database ( 1000 rows per insert, raw daily sales will act as a staging table)
   
   ##load good data
    sales_df.to_sql(name='raw_daily_sales',con=engine, if_exists='replace', index=False,chunksize=1000)

    ##load bad data to DLQ table
    DLQ_rows.to_sql(name='error_log',con=engine, if_exists='append', index=False,chunksize=1000)


    #third step T- transform, take data from staging table and insert into final table with transformations
    create_raw_sales_table_query = """
    CREATE TABLE IF NOT EXISTS raw_daily_sales (
    sale_id VARCHAR(50) PRIMARY KEY,
    transaction_date TEXT,
    amount DECIMAL(10, 2),
    region VARCHAR(50)
    );
    """

    #create error_Log table for bad data, all char beacsue we want it to be as simple as possible- juet let the rows in 
    create_error_table = """
    CREATE TABLE IF NOT EXISTS error_log (
    sale_id VARCHAR(255),
    transaction_date VARCHAR(255), 
    amount VARCHAR(255),
    region VARCHAR(255),
    error_reason VARCHAR(255),      -- Extra column for why it failed
    ingestion_time TIMESTAMP DEFAULT NOW() -- Extra column for when it happened
    );
    """

    #creating the table since its a learning project, in production you would have this already created    
    create_sales_table_query = """
    CREATE TABLE IF NOT EXISTS daily_sales (
    sale_id VARCHAR(50) PRIMARY KEY,
    transaction_date TEXT,
    amount DECIMAL(10, 2),
    region VARCHAR(50)
    );
    """
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(sa.text(create_raw_sales_table_query))
            connection.execute(sa.text(create_error_table))
            connection.execute(sa.text(create_sales_table_query))

    sql_upsert_logic = """
    INSERT INTO daily_sales (sale_id, transaction_date, amount, region)
    SELECT sale_id, transaction_date, amount, region 
    FROM raw_daily_sales
    ON CONFLICT (sale_id) 
    DO UPDATE SET
        transaction_date = EXCLUDED.transaction_date,
        amount = EXCLUDED.amount,
        region = EXCLUDED.region;
    """
    print("Starting upsert logic...")    
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(sa.text(sql_upsert_logic))
    print("ETL process completed successfully.")
            
