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
    	IFNULL(t_bike_order_record.id,0) AS id, 
    	IFNULL(t_bike_order_record.imei,0) AS imei, 
    	IFNULL(t_bike_order_record.order_id,0) AS order_id, 
    	IFNULL(t_bike_order_record.`time`,0) AS `time`,
    	IFNULL(t_bike_order_record.content,0) AS content, 
    	IFNULL(t_bike_order_record.`from`,0) AS `from`
    FROM shamri.t_bike_order_record
    WHERE 
    	t_bike_order_record.id > {max_id_postgres}
    '''.format(max_id_postgres=max_id_postgres)

    df_fresh_t_bike_order_record_mysql = pd.read_sql(select_fresh_t_bike_order_record_mysql, engine_mysql)

    # Загрузка свежих данных t_bike_order_record в Postgres
    df_fresh_t_bike_order_record_mysql.replace('', '0').to_sql("t_bike_order_record", engine_postgresql, if_exists="append", index=False)

    
    print('Added {x} records to t_bike_order_record in Postgres!'.format(x = df_fresh_t_bike_order_record_mysql.shape[0]))

    #   Обновление таблицы t_bike в Postgres. Начало
    # Копирую t_bike
    select_t_bike = '''SELECT
                            NOW() as 'timestamp',
                            IFNULL(t_bike.id,0) AS id,
                            IFNULL(t_bike.number,0) AS number,
                            IFNULL(t_bike.imei,0) AS imei,
                            IFNULL(t_bike.type_id,0) AS type_id,
                            IFNULL(t_bike.g_time,0) AS g_time,
                            IFNULL(t_bike.g_lat,0) AS g_lat,
                            IFNULL(t_bike.g_lng,0) AS g_lng,
                            IFNULL(t_bike.status,0) AS status,
                            IFNULL(t_bike.use_status,0) AS use_status,
                            IFNULL(t_bike.power,0) AS power,
                            IFNULL(t_bike.gsm,0) AS gsm,
                            IFNULL(t_bike.gps_number,'empty') AS gps_number,
                            IFNULL(t_bike.city_id,0) AS city_id,
                            IFNULL(t_bike.heart_time,0) AS heart_time,
                            IFNULL(t_bike.version,0) AS version,
                            IFNULL(t_bike.version_time,0) AS version_time,
                            IFNULL(t_bike.readpack,0) AS readpack,
                            IFNULL(t_bike.add_date, STR_TO_DATE("2024-01-01 00:00:00", "%Y-%m-%d %H:%i:%s")) AS add_date,
                            IFNULL(t_bike.error_status,0) AS error_status,
                            IFNULL(t_bike.server_ip,'0.0.0.0') AS server_ip,
                            IFNULL(t_bike.bike_status,0) AS bike_status,
                            IFNULL(t_bike.sponsors_id,0) AS sponsors_id,
                            IFNULL(t_bike.bike_no,0) AS bike_no,
                            IFNULL(t_bike.bike_type,0) AS bike_type,
                            IFNULL(t_bike.extend_info,0) AS extend_info,
                            IFNULL(t_bike.area_id,0) AS area_id,
                            IFNULL(t_bike.bike_power,0) AS bike_power,
                            IFNULL(t_bike.bike_power_status,0) AS bike_power_status,
                            IFNULL(t_bike.mac,0) AS mac,
                            IFNULL(t_bike.iccid,'empty') AS iccid,
                            IFNULL(t_bike.maintain_status,0) AS maintain_status,
                            IFNULL(t_bike.extra_lock_status,0) AS extra_lock_status,
                            IFNULL(t_bike.available,0) AS available,
                            IFNULL(t_bike.model,'empty') AS model,
                            IFNULL(t_bike.protocol,0) AS protocol,
                            IFNULL(t_bike.frame_number,'empty') AS frame_number,
                            IFNULL(t_bike.battery_key,0) AS battery_key,
                            IFNULL(t_bike.release_time, STR_TO_DATE("2024-01-01 00:00:00", "%Y-%m-%d %H:%i:%s")) AS release_time,
                            IFNULL(t_bike.last_service_time, STR_TO_DATE("2024-01-01 00:00:00", "%Y-%m-%d %H:%i:%s")) AS last_service_time,
                            IFNULL(t_bike.industry_id,0) AS industry_id,
                            IFNULL(t_bike.ble_key,'empty') AS ble_key,
                            IFNULL(t_bike.user_group_id,0) AS user_group_id 
                            FROM shamri.t_bike
    '''

    df_t_bike = pd.read_sql(select_t_bike, engine_mysql)

    # Очистка таблицы в Postgres
    truncate_t_bike = "TRUNCATE TABLE t_bike RESTART IDENTITY;"
    with engine_postgresql.connect() as connection:
        with connection.begin() as transaction:
            print(f"Попытка очистить таблицу")
            # Очистка t_bike
            connection.execute(sa.text(truncate_t_bike))
            # Если ошибок нет, транзакция фиксируется автоматически
            print(f"Таблица t_bike успешно очищена!")

    df_t_bike.to_sql("t_bike", engine_postgresql, if_exists="append", index=False)
    print('Таблица t_bike успешно обновлена!')

    #   Обновление таблицы t_bike в Postgres. Конец

if __name__ == "__main__":
    main()
