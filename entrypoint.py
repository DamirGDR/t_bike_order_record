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
    max_id_postgres = int(df_max_id_postgres.iloc[0].iloc[0])

    # Выгрузка свежих данных t_bike_order_record из MYSQL
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

    #   Обновление таблицы t_bike_use в Postgres. Начало
    # Максимальный id записи в принимающей таблице
    select_max_id_t_bike_use = '''
    SELECT 
    	MAX(id)
    FROM damir.t_bike_use
    '''
    df_max_id_t_bike_use_postgres = pd.read_sql(select_max_id_t_bike_use, engine_postgresql)
    max_id_t_bike_use = int(df_max_id_t_bike_use_postgres.iloc[0].iloc[0])

    select_fresh_t_bike_use_mysql = '''
            SELECT
                NOW() AS add_time,
                IfNULL(t_bike_use.id,0) AS id,
                IfNULL(t_bike_use.uid,0) AS uid,
                IfNULL(t_bike_use.bid,0) AS bid,
                IfNULL(t_bike_use.start_time,0) AS start_time,
                IfNULL(t_bike_use.end_time,0) AS end_time,
                IfNULL(t_bike_use.duration,0) AS duration ,
                IfNULL(t_bike_use.distance,0) AS distance ,
                IfNULL(t_bike_use.orbit,0) AS orbit,
                IfNULL(t_bike_use.start_lat,0) AS start_lat,
                IfNULL(t_bike_use.start_lng,0) AS start_lng,
                IfNULL(t_bike_use.end_lat,0) AS end_lat,
                IfNULL(t_bike_use.end_lng,0) AS end_lng,
                IfNULL(t_bike_use.ispay,0) AS ispay,
                IfNULL(t_bike_use.`date`,0) AS `date`,
                IfNULL(t_bike_use.lock_location,0) AS lock_location,
                IfNULL(t_bike_use.out_area,0) AS out_area,
                IfNULL(t_bike_use.open_way,0) AS open_way,
                IfNULL(t_bike_use.ride_amount,0) AS ride_amount,
                IfNULL(t_bike_use.ride_status,0) AS ride_status,
                IfNULL(t_bike_use.old_date,0) AS old_date,
                IfNULL(t_bike_use.close_way,0) AS close_way,
                IfNULL(t_bike_use.old_duration,0) AS old_duration,
                IfNULL(t_bike_use.admin_id,0) AS admin_id,
                IfNULL(t_bike_use.update_time,0) AS update_time,
                IfNULL(t_bike_use.lock_time,0) AS lock_time, 
                IfNULL(t_bike_use.host_id,0) AS host_id,
                IfNULL(t_bike_use.ride_user,0) AS ride_user,
                IfNULL(t_bike_use.group_ride,0) AS group_ride,
                IfNULL(t_bike_use.start_area,0) AS start_area,
                IfNULL(t_bike_use.end_area,0) AS end_area,
                IfNULL(t_bike_use.stripe_charge,0) AS stripe_charge,
                IfNULL(t_bike_use.stripe_refund,0) AS stripe_refund,
                IfNULL(t_bike_use.pause_duration,0) AS pause_duration,
                IfNULL(t_bike_use.discount,0) AS discount,
                IfNULL(t_bike_use.subscription_id,0) AS subscription_id,
                IfNULL(t_bike_use.subscription_mapping_id,0) AS subscription_mapping_id,
                IfNULL(t_bike_use.route_image,0) AS route_image,
                IfNULL(t_bike_use.parking_image,0) AS parking_image,
                IfNULL(t_bike_use.force_stop,0) AS force_stop,
                IfNULL(t_bike_use.force_stop_comment,0) AS force_stop_comment,
                IfNULL(t_bike_use.lights,0) AS lights,
                IfNULL(t_bike_use.gear,0) AS gear,
                IfNULL(t_bike_use.sent_unlock_time,0) AS sent_unlock_time,
                IfNULL(t_bike_use.recalculated,0) AS recalculated,
                IfNULL(t_bike_use.notified,0) AS notified,
                IfNULL(t_bike_use.notified_time,0) AS notified_time,
                IfNULL(t_bike_use.speed_zone_id,0) AS speed_zone_id,
                IfNULL(t_bike_use.admin_note,0) AS admin_note,
                IfNULL(t_bike_use.subscription_payment_id,0) AS subscription_payment_id,
                IfNULL(t_bike_use.subscr_paid_before_ride_balance,0) AS subscr_paid_before_ride_balance
            FROM shamri.t_bike_use
            WHERE t_bike_use.id > {max_id_t_bike_use}
                    '''.format(max_id_t_bike_use=max_id_t_bike_use)

    df_fresh_t_bike_use_mysql = pd.read_sql(select_fresh_t_bike_use_mysql, engine_mysql)
    df_fresh_t_bike_use_mysql.replace('', '0').to_sql("t_bike_use", engine_postgresql, if_exists="append", index=False)
    print('Added {x} records to t_bike_use in Postgres!'.format(x=df_fresh_t_bike_use_mysql.shape[0]))

    #   Обновление таблицы t_bike_use в Postgres. Конец

    #   Обновление таблицы t_ride_event_log в Postgres. Начало
    # Максимальный id записи в принимающей таблице
    select_max_id_t_ride_event_log = '''
    SELECT 
        MAX(id)
    FROM damir.t_ride_event_log
    '''
    df_max_id_t_ride_event_log_postgres = pd.read_sql(select_max_id_t_ride_event_log, engine_postgresql)
    max_id_t_ride_event_log = int(df_max_id_t_ride_event_log_postgres.iloc[0].iloc[0])

    select_fresh_t_ride_event_log_mysql = '''
            SELECT
                NOW() AS add_time,
                t_ride_event_log.id,
                t_ride_event_log.ride_id,
                t_ride_event_log.event,
                t_ride_event_log.description,
                t_ride_event_log.created 
            FROM shamri.t_ride_event_log
            WHERE t_ride_event_log.id > {max_id_t_ride_event_log}
                    '''.format(max_id_t_ride_event_log=max_id_t_ride_event_log)

    df_fresh_t_ride_event_log_mysql = pd.read_sql(select_fresh_t_ride_event_log_mysql, engine_mysql)
    df_fresh_t_ride_event_log_mysql.replace('', '0').to_sql("t_ride_event_log", engine_postgresql, if_exists="append", index=False)
    print('Added {x} records to t_bike_use in Postgres!'.format(x = df_fresh_t_ride_event_log_mysql.shape[0]))

    #   Обновление таблицы t_ride_event_log в Postgres. Конец



if __name__ == "__main__":
    main()
