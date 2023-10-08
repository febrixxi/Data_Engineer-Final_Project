import pandas as pd
from sqlalchemy import create_engine
import logging

class Transformer():
    def __init__(self, mysql_conn, postgres_conn):
        self.mysql_conn = mysql_conn
        self.postgres_conn = postgres_conn
    
    def create_dimension_district(self):
        query = 'select distinct kode_kab as district_id, kode_prov as province_id, nama_kab as district_name from covid_jabar'
        df = pd.read_sql(query, self.mysql_conn)

        try:
            p = 'DROP table if exists dim_district'
            self.postgres_conn.execute(p)
        except Exception as e:
            logging.error(e)

        df.to_sql(con=self.postgres_conn, name='dim_district', index=False)
        print('DIM DISTRICT INSERTED SUCCESSFULLY TO DWH')

    
    def create_dimension_province(self):
        query = 'select distinct kode_prov as province_id, nama_prov as province_name from covid_jabar'
        df = pd.read_sql(query, self.mysql_conn)

        try:
            p = 'DROP table if exists dim_province'
            self.postgres_conn.execute(p)
        except Exception as e:
            logging.error(e)

        df.to_sql(con=self.postgres_conn, name='dim_province', index=False)
        print('DIM PROVINCE INSERTED SUCCESSFULLY TO DWH')
    
    def create_dimension_case(self):
        query = 'select * from covid_jabar'
        df = pd.read_sql(query, self.mysql_conn)

        status = [column for column in df.columns if '_' in column and 'kode' not in column and 'nama' not in column]
        status_name = [i.rsplit('_')[0] for i in status]
        status_detail = [i.rsplit('_')[-1] for i in status]
        id = [i+1 for i in range(len(status))]

        data = {'id': id, 'status_name': status_name, 'status_detail': status_detail, 'status': status}
        df = pd.DataFrame(data)

        try:
            p = 'DROP table if exists dim_case'
            self.postgres_conn.execute(p)
        except Exception as e:
            logging.error(e)
        
        df.to_sql(con=self.postgres_conn, name='dim_case', index=False)
        print('DIM CASE INSERTED SUCCESSFULLY TO DWH')
        return df
    
    def create_province_daily(self):
        query = 'select * from covid_jabar'
        dim_case = self.create_dimension_case()

        df = pd.read_sql(query, self.mysql_conn)
        df = df.drop(columns=['CLOSECONTACT','CONFIRMATION','PROBABLE','SUSPECT','kode_kab','nama_kab','nama_prov'])

        df_new = (df.set_index(["kode_prov", "tanggal"])
                    .stack()
                    .reset_index(name='value')
                    .rename(columns={'level_2':'status'}))        
        df_new = df_new.merge(dim_case, on='status', how='inner')
        df_new = df_new.drop(columns=['status','status_name','status_detail'])\
                       .rename(columns={'id':'case_id','kode_prov':'province_id','tanggal':'date','value':'total'})
        df_final = df_new.groupby(['province_id','case_id','date'])['total']\
                         .sum()\
                         .reset_index()
        df_final['id'] = range(1, len(df_final)+1)
        df_final = df_final[['id','province_id','case_id','date','total']]
        
        try:
            p = 'DROP table if exists province_daily'
            self.postgres_conn.execute(p)
        except Exception as e:
            logging.error(e)
        
        df_final.to_sql(con=self.postgres_conn, name='province_daily', index=False)
        print('FACT PROVINCE DAILY INSERTED SUCCESSFULLY TO DWH')
        
    def create_district_daily(self):
        query = 'select * from covid_jabar'
        dim_case = self.create_dimension_case()

        df = pd.read_sql(query, self.mysql_conn)
        df = df.drop(columns=['CLOSECONTACT','CONFIRMATION','PROBABLE','SUSPECT','kode_prov','nama_kab','nama_prov'])

        df_new = (df.set_index(["kode_kab", "tanggal"])
                    .stack()
                    .reset_index(name='value')
                    .rename(columns={'level_2':'status'}))        
        df_new = df_new.merge(dim_case, on='status', how='inner')
        df_new = df_new.drop(columns=['status','status_name','status_detail'])\
                       .rename(columns={'id':'case_id','kode_kab':'district_id','tanggal':'date','value':'total'})
        df_final = df_new.groupby(['district_id','case_id','date'])['total']\
                         .sum()\
                         .reset_index()\
                         .sort_values(by=['case_id','date','district_id'])
        df_final['id'] = range(1, len(df_final)+1)
        df_final = df_final[['id','district_id','case_id','date','total']]

        try:
            p = 'DROP table if exists district_daily'
            self.postgres_conn.execute(p)
        except Exception as e:
            logging.error(e)
        
        df_final.to_sql(con=self.postgres_conn, name='district_daily', index=False)
        print('FACT DISTRICT DAILY INSERTED SUCCESSFULLY TO DWH')

    