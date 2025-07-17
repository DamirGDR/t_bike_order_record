import os

import pandas as pd
import sqlalchemy as sa
import json
import google.oauth2.service_account
import googleapiclient.discovery

# Секреты MySQL


def get_mysql_url() -> str:
    url = os.environ["mysql_url"]
    return url


def get_postgres_url() -> str:
    url = os.environ["postgres_url"]
    return url


def main():
    url = get_postgres_url()
    url = sa.engine.make_url(url)
    url = url.set(drivername="postgresql+psycopg")
    engine_postgresql = sa.create_engine(url)

    url = get_mysql_url()
    url = sa.engine.make_url(url)
    url = url.set(drivername="mysql+mysqlconnector")
    engine_mysql = sa.create_engine(url)

    # Максимальный id записи в принимающей таблице
    select_max_id_t_bike_order_record = '''
    SELECT 
    	MAX(id)
    FROM damir.t_bike_order_record
    '''
    df_max_id_postgres = pd.read_sql(select_max_id_t_bike_order_record, engine_postgresql)
    max_id_postgres = int(df_max_id_postgres.iloc[0])

    # Выгрузка свежих данных из MYSQL
    select_fresh_t_bike_order_record_mysql = '''
    SELECT 
    	NOW() AS add_time,
    	t_bike_order_record.id, 
    	t_bike_order_record.imei, 
    	t_bike_order_record.order_id, 
    	t_bike_order_record.`time`,
    	t_bike_order_record.content, 
    	t_bike_order_record.`from`
    FROM shamri.t_bike_order_record
    WHERE 
    	t_bike_order_record.id > {max_id_postgres}
    '''.format(max_id_postgres=max_id_postgres)

    df_fresh_t_bike_order_record_mysql = pd.read_sql(select_fresh_t_bike_order_record_mysql, engine_mysql)

    # Загрузка свежих данных t_bike_order_record в Postgres
    df_fresh_t_bike_order_record_mysql.to_sql("t_bike_order_record", engine_postgresql, if_exists="append", index=False)

    # # url = get_mysql_url()
    # # url = sa.engine.make_url(url)
    # # url = url.set(drivername="mysql+mysqlconnector")
    # # engine_mysql = sa.create_engine(url)
    # # df_vni = pd.read_sql(select_vni_total, engine_mysql)
    #
    #
    # # Загрузка за сегодня в Postgres
    # url = get_postgres_url()
    # url = sa.engine.make_url(url)
    # url = url.set(drivername="postgresql+psycopg")
    # engine_postgresql = sa.create_engine(url)
    # df_vni.to_sql("vni_total", engine_postgresql, if_exists="append", index=False)
    
    print('t_bike_order_record UPDATED!')


if __name__ == "__main__":
    main()
