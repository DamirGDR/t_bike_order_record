import os

import pandas as pd
import sqlalchemy as sa
import requests
import json
import google.oauth2.service_account
import googleapiclient.discovery

# Секреты


def get_mysql_url() -> str:
    url = os.environ["mysql_url"]
    return url


def get_postgres_url() -> str:
    url = os.environ["postgres_url"]
    return url


def get_token() -> str:
    res = os.environ["TOKEN"]
    return res


def get_chat_id() -> str:
    res = os.environ["chat_id"]
    return res


# Функция отправки сообщения в ТГ
def send_message_tg(TOKEN, chat_id, message_text):
    TOKEN = TOKEN
    chat_id = chat_id
    message = message_text
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
    return requests.get(url).json()


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
            print(f"Попытка очистить таблицу t_bike...")
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
    print('Added {x} records to t_ride_event_log in Postgres!'.format(x = df_fresh_t_ride_event_log_mysql.shape[0]))

    #   Обновление таблицы t_ride_event_log в Postgres. Конец

    #   Обновление таблицы alarms_1 в Postgres. Начало
    select_max_id_alarms_1 = '''
    SELECT 
        MAX(id)
    FROM damir.alarms_1
    '''
    df_max_id_alarms_1 = pd.read_sql(select_max_id_alarms_1, engine_postgresql)
    max_id_alarms_1 = int(df_max_id_alarms_1.iloc[0].iloc[0])
    select_fresh_alarms_1 = '''WITH detected_combinations AS (
                    SELECT
                        id,
                        imei,
                        event_time,
                        ARRAY_AGG(status_code) OVER (
                            PARTITION BY imei 
                            ORDER BY event_time 
                            RANGE BETWEEN CURRENT ROW AND INTERVAL '10 minutes' FOLLOWING
                        ) AS combination_array
                    FROM
                        (
                        SELECT
                            id,
                            imei,
                            to_timestamp("time") AS event_time, 
                            CAST(substring(t_bike_order_record."content" FROM 'status:([0-9]+)') AS integer) AS status_code
                        FROM
                            damir.t_bike_order_record
                        WHERE
                            content IN ('status:1', 'status:2', 'status:6')
                            -- AND t_bike_order_record."time" >= extract(epoch from CURRENT_DATE)
                            AND t_bike_order_record.id > {}
                        ) AS alarm_events
                ),
                combinations_result AS (
                -- Выбираю только первые уникальные появления нужных комбинаций
                SELECT
                    imei,
                    event_time AS detection_time,
                    combination_array AS combination
                FROM (
                    SELECT
                        imei,
                        event_time,
                        combination_array,
                        LAG(combination_array, 1, '{{}}'::int[]) OVER (
                            PARTITION BY imei ORDER BY event_time
                        ) AS prev_combination
                    FROM
                        detected_combinations
                ) AS final_results
                WHERE
                    combination_array <> prev_combination
                    AND
                    (
                        (combination_array @> ARRAY[1, 2, 6]) OR
                        (array_length(combination_array, 1) = 2 AND combination_array @> ARRAY[1, 2]) OR
                        (array_length(combination_array, 1) = 2 AND combination_array @> ARRAY[1, 6]) OR
                        (array_length(combination_array, 1) = 2 AND combination_array @> ARRAY[2, 6]) OR
                        (array_length(combination_array, 1) = 1 AND combination_array @> ARRAY[6])
                    )
                    ),
                mr_admin_samokat_razblokirovan AS (
                    SELECT
                        t_bike.imei AS imei_mr_admin_samokat_razblokirovan,
                        t_bike.g_lat AS g_lat_mr_admin_samokat_razblokirovan,
                        t_bike.g_lng AS g_lng_mr_admin_samokat_razblokirovan
                    FROM
                        damir.t_bike
                    WHERE t_bike.error_status = 1 
                        AND t_bike.bike_type = 2 
                        AND t_bike.status = 1
                        AND to_timestamp(t_bike.heart_time) > NOW() - interval '900 seconds'
                ),
                trevogi AS (
                    SELECT
                        combinations_result.*,
                        t_bike.g_lat,
                        t_bike.g_lng,
                        mr_admin_samokat_razblokirovan.imei_mr_admin_samokat_razblokirovan,
                        mr_admin_samokat_razblokirovan.g_lat_mr_admin_samokat_razblokirovan,
                        mr_admin_samokat_razblokirovan.g_lng_mr_admin_samokat_razblokirovan,
                        sqrt(111206.4 * (mr_admin_samokat_razblokirovan.g_lat_mr_admin_samokat_razblokirovan - t_bike.g_lat) * 111206.4 * (mr_admin_samokat_razblokirovan.g_lat_mr_admin_samokat_razblokirovan - t_bike.g_lat) + 111206.4 * (mr_admin_samokat_razblokirovan.g_lng_mr_admin_samokat_razblokirovan - t_bike.g_lng) * cos(t_bike.g_lat / 57.3) * 111206.4 * (mr_admin_samokat_razblokirovan.g_lng_mr_admin_samokat_razblokirovan - t_bike.g_lng) * cos(t_bike.g_lat / 57.3)) AS distance_in_m
                    FROM combinations_result
                    LEFT JOIN damir.t_bike ON combinations_result.imei=t_bike.imei
                    CROSS JOIN mr_admin_samokat_razblokirovan
                ),
                distinct_trevogi AS (
                    SELECT DISTINCT ON (trevogi.imei, trevogi.detection_time, trevogi.g_lat, trevogi.g_lng)
                        trevogi.imei,
                        trevogi.detection_time,
                        trevogi.combination,
                        extract(epoch from trevogi.detection_time) AS detection_time_timestamp,
                        trevogi.g_lat,
                        trevogi.g_lng,
                        TRUE AS flag
                    FROM trevogi
                ),
                t_bike_order_record_trevogi AS (
                    SELECT 
                        t_bike_order_record.add_time,
                        t_bike_order_record.id,
                        t_bike_order_record.imei,
                        t_bike_order_record.order_id,
                        t_bike_order_record."time",
                        split_part(split_part(t_bike_order_record."content", ',', 1), '=', 2)::numeric AS lat,
                        split_part(split_part(t_bike_order_record."content", ',', 2), '=', 2)::numeric AS lng,
                        t_bike_order_record."from",
                        distinct_trevogi.detection_time,
                        distinct_trevogi.detection_time_timestamp,
                        distinct_trevogi.flag,
                        distinct_trevogi.combination
                    FROM damir.t_bike_order_record
                    LEFT JOIN distinct_trevogi 
                    ON t_bike_order_record.imei = distinct_trevogi.imei AND t_bike_order_record."time" = distinct_trevogi.detection_time_timestamp
                    WHERE t_bike_order_record.imei IN (SELECT DISTINCT distinct_trevogi.imei FROM distinct_trevogi) 
                        -- AND t_bike_order_record."time" <= (SELECT MAX(distinct_trevogi.detection_time_timestamp) FROM distinct_trevogi)
                        AND t_bike_order_record."content" LIKE '%%lat%%' 
                        -- AND t_bike_order_record."time" >= extract(epoch from CURRENT_DATE)
                        AND t_bike_order_record.id > {}
                ),
                t_bike_order_record_trevogi_with_temp_distances AS (
                    SELECT
                        t_bike_order_record_trevogi.*,
                                sqrt(111206.4 * (LAG(t_bike_order_record_trevogi.lat,1) OVER (PARTITION BY t_bike_order_record_trevogi.imei ORDER BY t_bike_order_record_trevogi."time") - t_bike_order_record_trevogi.lat) * 111206.4 * (LAG(t_bike_order_record_trevogi.lat,1) OVER (PARTITION BY t_bike_order_record_trevogi.imei ORDER BY t_bike_order_record_trevogi."time") - t_bike_order_record_trevogi.lat) + 111206.4 * (LAG(t_bike_order_record_trevogi.lng,1) OVER (PARTITION BY t_bike_order_record_trevogi.imei ORDER BY t_bike_order_record_trevogi."time") - t_bike_order_record_trevogi.lng) * cos(t_bike_order_record_trevogi.lat / 57.3) * 111206.4 * (LAG(t_bike_order_record_trevogi.lng,1) OVER (PARTITION BY t_bike_order_record_trevogi.imei ORDER BY t_bike_order_record_trevogi."time") - t_bike_order_record_trevogi.lng) * cos(t_bike_order_record_trevogi.lat / 57.3)) AS temp_distance
                    FROM t_bike_order_record_trevogi
                )
                SELECT
                    NOW() AS add_time,
                    alarm_tab.id,
                    alarm_tab.imei,
                    t_bike."number",
                    alarm_tab.detection_time,
                    alarm_tab.lat,
                    alarm_tab.lng,
                    alarm_tab.combination,
                    alarm_tab.distance_prev_3_min,
                    t_bike.g_lat,
                    t_bike.g_lng,
                    CONCAT('https://www.google.com/maps?q=', alarm_tab.lat, ',', alarm_tab.lng)  AS first_alarm_location,
                    CONCAT('https://www.google.com/maps?q=', t_bike.g_lat, ',', t_bike.g_lng)  AS now_location
                FROM 
                    (
                    SELECT
                        DISTINCT ON (raw_table.imei, raw_table.detection_time)
                        raw_table.*
                    FROM 
                        (SELECT 
                            t_bike_order_record_trevogi_with_temp_distances.*,
                            -- to_timestamp(t_bike_order_record_trevogi_with_temp_distances."time") AS event_time,
                            SUM(t_bike_order_record_trevogi_with_temp_distances.temp_distance) OVER (
                                PARTITION BY t_bike_order_record_trevogi_with_temp_distances.imei 
                                ORDER BY to_timestamp(t_bike_order_record_trevogi_with_temp_distances."time")
                                RANGE BETWEEN INTERVAL '3 minutes' PRECEDING AND CURRENT ROW
                                ) AS distance_prev_3_min
                        FROM t_bike_order_record_trevogi_with_temp_distances
                        ) AS raw_table
                    WHERE raw_table.flag = TRUE
                        AND distance_prev_3_min > 50
                    ORDER BY 
                        raw_table.imei, 
                        raw_table.detection_time
                        ) AS alarm_tab
                LEFT JOIN damir.t_bike ON alarm_tab.imei = t_bike.imei
                ORDER BY alarm_tab.detection_time ASC'''.format(max_id_alarms_1, max_id_alarms_1)
    df_fresh_alarms_1 = pd.read_sql(select_fresh_alarms_1, engine_postgresql)
    df_fresh_alarms_1.replace('', '0').to_sql("alarms_1", engine_postgresql, if_exists="append", index=False)
    print('Added {x} records to alarms_1 in Postgres!'.format(x=df_fresh_alarms_1.shape[0]))
    #   Обновление таблицы alarms_1 в Postgres. Конец

    #   Обновление таблицы alarms_2 в Postgres. Начало
    select_max_id_alarms_2 = '''
    SELECT 
        MAX(id)
    FROM damir.alarms_2
    '''
    df_max_id_alarms_2 = pd.read_sql(select_max_id_alarms_2, engine_postgresql)
    max_id_alarms_2 = int(df_max_id_alarms_2.iloc[0].iloc[0])
    select_fresh_alarms_2 = '''
        WITH prev_rides AS (
            SELECT
                t_bike_use.*,
                LAG(t_bike_use.ride_status) OVER (PARTITION BY t_bike_use.bid ORDER BY t_bike_use.date) as prev_status,
                LAG(t_bike_use.ride_status, 2) OVER (PARTITION BY t_bike_use.bid ORDER BY t_bike_use.date) as prev_prev_status
            FROM
                damir.t_bike_use
            WHERE t_bike_use.id > {max_id_alarms_2}
        )
        SELECT DISTINCT ON (prev_rides.id, to_timestamp(prev_rides.date), t_bike.number)
            prev_rides.id,
            to_timestamp(prev_rides.date) AS start_timestamp,
            t_bike.number,
            t_bike.imei,
            prev_rides.start_lat,
            prev_rides.start_lng
        FROM
            prev_rides
        LEFT JOIN damir.t_bike ON prev_rides.bid = t_bike.id
        WHERE
            ride_status = 7
            AND prev_status = 7
            AND prev_prev_status = 7
        ORDER BY to_timestamp(prev_rides.date) ASC
    '''.format(max_id_alarms_2=max_id_alarms_2)
    df_fresh_alarms_2 = pd.read_sql(select_fresh_alarms_2, engine_postgresql)
    df_fresh_alarms_2.replace('', '0').to_sql("alarms_2", engine_postgresql, if_exists="append", index=False)
    print('Added {x} records to alarms_2 in Postgres!'.format(x=df_fresh_alarms_2.shape[0]))
    #   Обновление таблицы alarms_2 в Postgres. Конец

    #   Обновление таблицы alarms_3 в Postgres. Начало
    select_max_id_alarms_3 = '''
    SELECT 
        MAX(id)
    FROM damir.alarms_3
    '''
    df_max_id_alarms_3 = pd.read_sql(select_max_id_alarms_3, engine_postgresql)
    max_id_alarms_3 = int(df_max_id_alarms_3.iloc[0].iloc[0])
    select_fresh_alarms_3 = '''
        SELECT 
            t_bike_use_temp.id,
            t_bike_use_temp.start_timestamp ,
            t_bike_use_temp.uid,
            t_bike.number,
            t_bike.imei,
            t_bike_use_temp.start_lat,
            t_bike_use_temp.start_lng
        FROM 
            (
            SELECT
                to_timestamp(t_bike_use.start_time) AS start_timestamp,
                t_bike_use.*,
                LAG(t_bike_use.ride_status) OVER (PARTITION BY t_bike_use.uid ORDER BY t_bike_use.date) as prev_status,
                LAG(t_bike_use.ride_status, 2) OVER (PARTITION BY t_bike_use.uid ORDER BY t_bike_use.date) as prev_prev_status
            FROM t_bike_use
            WHERE t_bike_use.id > {max_id_alarms_3}
            ) AS t_bike_use_temp
        LEFT JOIN t_bike ON t_bike_use_temp.bid = t_bike.id
        WHERE t_bike_use_temp.ride_status = 3
            AND t_bike_use_temp.prev_status = 7
            AND t_bike_use_temp.prev_prev_status = 7
        ORDER BY t_bike_use_temp.start_time ASC
    '''.format(max_id_alarms_3=max_id_alarms_3)
    df_fresh_alarms_3 = pd.read_sql(select_fresh_alarms_3, engine_postgresql)
    df_fresh_alarms_3.replace('', '0').to_sql("alarms_3", engine_postgresql, if_exists="append", index=False)
    print('Added {x} records to alarms_3 in Postgres!'.format(x=df_fresh_alarms_3.shape[0]))
    #   Обновление таблицы alarms_3 в Postgres. Конец

    #   Обновление таблицы alarms_4 в Postgres. Начало
    select_max_id_alarms_4 = '''
    SELECT 
        MAX(id)
    FROM damir.alarms_4
    '''
    df_max_id_alarms_4 = pd.read_sql(select_max_id_alarms_4, engine_postgresql)
    max_id_alarms_4 = int(df_max_id_alarms_4.iloc[0].iloc[0])
    select_fresh_alarms_4 = '''
        SELECT 
            raw.*
        FROM 
            (
            SELECT 
                t_ride_event_log.id,
                t_ride_event_log.created,
                t_bike_use.uid,
                array_length(ARRAY_AGG(t_ride_event_log."event") OVER (
                        PARTITION BY uid 
                        ORDER BY created 
                        RANGE BETWEEN INTERVAL '10 minutes' PRECEDING AND CURRENT ROW
                    ), 1) AS kolvo_otmen_prev_10_min
            FROM t_ride_event_log
            LEFT JOIN t_bike_use ON t_ride_event_log.ride_id = t_bike_use.id
            WHERE t_ride_event_log.event = 'Canceled' 
                AND t_bike_use.duration < 60
                AND t_ride_event_log.id > {max_id_alarms_4}
            ) AS raw
        WHERE raw.kolvo_otmen_prev_10_min >= 3
        ORDER BY raw.created ASC
    '''.format(max_id_alarms_4=max_id_alarms_4)
    df_fresh_alarms_4 = pd.read_sql(select_fresh_alarms_4, engine_postgresql)
    df_fresh_alarms_4.replace('', '0').to_sql("alarms_4", engine_postgresql, if_exists="append", index=False)
    print('Added {x} records to alarms_4 in Postgres!'.format(x=df_fresh_alarms_4.shape[0]))
    #   Обновление таблицы alarms_4 в Postgres. Конец


    # Отправка alarms_1 в тг
    TOKEN = get_token()
    chat_id = get_chat_id()

    select_unsent_records = '''
    SELECT *
    FROM damir.alarms_1
    WHERE is_message_sent IS NULL OR is_message_sent = ''
    '''
    df_unsent_records = pd.read_sql(select_unsent_records, engine_postgresql)

    for id in df_unsent_records['id']:
        sim_number = df_unsent_records.loc[(df_unsent_records['id'] == id), 'number'].iloc[0]

        message_1 = 'https://www.google.com/maps?q=' + \
                    str(df_unsent_records.loc[(df_unsent_records['id'] == id), 'lat'].iloc[0]) + \
                    ',' + \
                    str(df_unsent_records.loc[(df_unsent_records['id'] == id), 'lng'].iloc[0])
        message_1_ = 'https://maps.google.com/maps?q=' + \
                     str(df_unsent_records.loc[(df_unsent_records['id'] == id), 'lat'].iloc[0]) + \
                     ',' + \
                     str(df_unsent_records.loc[(df_unsent_records['id'] == id), 'lng'].iloc[0]) + \
                     '&ll=' + \
                     str(df_unsent_records.loc[(df_unsent_records['id'] == id), 'lat'].iloc[0]) + \
                     ',' + \
                     str(df_unsent_records.loc[(df_unsent_records['id'] == id), 'lng'].iloc[0]) + \
                     '&z=16'
        message_2 = f'Внимание! Несанкционированное передвижение {sim_number}'.format(sim_number)
        print(message_1)
        print(message_2)
        send_message_tg(TOKEN, chat_id, message_1)
        send_message_tg(TOKEN, chat_id, message_2)
        insert_example = '''
                    UPDATE damir.alarms_1
                    SET is_message_sent = '1'
                    WHERE id = {id}
                    '''.format(id=str(id))


        with engine_postgresql.connect() as connection:
            with connection.begin() as transaction:
                connection.execute(sa.text(insert_example))

if __name__ == "__main__":
    main()
