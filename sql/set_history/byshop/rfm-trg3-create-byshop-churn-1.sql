CREATE OR REPLACE TABLE ballosodeuk.ynam.rfm_table_byshop_before_survive_prop AS (

WITH order_complete AS (
    SELECT 
        event_dt, 
        event_dttm, 
        user_id, 
        airbridge_device_id, 
        platform,
        transaction_id, 
        event_category, 
        event_label, 
        event_action, 
        price as event_value
    FROM `ballosodeuk.dw.fact_airbridge_event_order`
    WHERE 
        DATE(event_dt) BETWEEN date("2024-03-01") AND date({end_date})
        AND event_action NOT LIKE "^PAY" 
        AND event_label != "쇼핑"
        AND user_id NOT LIKE 'IU_%' 
        AND user_id IS NOT NULL
),

shortcut_order AS (
    SELECT 
        event_dt, 
        user_id, 
        MIN(airbridge_device_id) as airbridge_device_id, 
        transaction_id, 
        MIN(platform) as platform, 
        event_label, 
        event_action, 
        MIN(event_category) as event_category, 
        SUM(Total_Revenue) as Total_Revenue
    FROM( 
        SELECT 
            event_dt, 
            event_dttm, 
            user_id, 
            airbridge_device_id, 
            platform,
            transaction_id, 
            event_category, 
            event_action, 
            CASE 
                WHEN event_action IN ('balso1sa1','balso1sr1','balso2sr1','balso2sa2') THEN '바로가기'
                WHEN event_action = 'balso1sr2' THEN '퀴즈쿠팡'
                WHEN event_action = 'balso2sa1' THEN '챌린지인증쿠팡'
            END AS event_label, 
            ROUND(SUM(event_value) * 0.046) as Total_Revenue
        FROM order_complete
        WHERE event_category = "Order Complete (App)"
        GROUP BY 
            event_dt, 
            event_dttm, 
            user_id, 
            airbridge_device_id,
            transaction_id, 
            platform, 
            event_category, 
            event_label, 
            event_action
    )
    GROUP BY 
        event_dt, 
        user_id, 
        transaction_id, 
        event_label, 
        event_action
),

shortcut_refund AS (
    SELECT 
        event_dt, 
        user_id, 
        MIN(airbridge_device_id) as airbridge_device_id, 
        transaction_id, 
        MIN(platform) as platform, 
        event_label, 
        event_action, 
        MIN(event_category) as event_category, 
        SUM(Total_Revenue) as Total_Revenue
    FROM( 
        SELECT 
            event_dt, 
            event_dttm, 
            user_id, 
            airbridge_device_id, 
            platform,
            transaction_id, 
            event_category, 
            event_action, 
            CASE 
                WHEN event_action IN ('balso1sa1','balso1sr1','balso2sr1','balso2sa2') THEN '바로가기'
                WHEN event_action = 'balso1sr2' THEN '퀴즈쿠팡'
                WHEN event_action = 'balso2sa1' THEN '챌린지인증쿠팡'
            END AS event_label, 
            ROUND(SUM(event_value) * 0.046) * -1 as Total_Revenue
        FROM order_complete
        WHERE event_category = "Order Cancel (App)"
        GROUP BY 
            event_dt, 
            event_dttm, 
            user_id, 
            airbridge_device_id,
            transaction_id, 
            platform, 
            event_category, 
            event_label, 
            event_action
    )
    GROUP BY 
        event_dt, 
        user_id, 
        transaction_id, 
        event_label, 
        event_action
),

shortcut AS (
    SELECT 
        o.event_dt, 
        o.user_id, 
        o.airbridge_device_id, 
        o.transaction_id, 
        o.platform, 
        o.event_label, 
        o.event_action, 
        o.event_category, 
        o.Total_Revenue + COALESCE(r.Total_Revenue, 0) as Total_Revenue
    FROM shortcut_order o
    LEFT JOIN shortcut_refund r 
        ON o.user_id = r.user_id 
        AND o.transaction_id = r.transaction_id
),

dynamic_order AS (
    SELECT 
        CAST(FORMAT_DATE('%Y-%m-%d', DATETIME(date)) AS Date) as event_dt, 
        subParam AS user_id, 
        'Order Complete (App)' AS event_category, 
        'dynamic' AS event_action, 
        'coin' As event_label,
        SUM(commission) AS Total_Revenue
    FROM `ballosodeuk.external_mart.cpDynamic_orders` 
    WHERE 
        date BETWEEN date("2024-06-19") AND date({end_date})
        AND subParam IS NOT NULL
    GROUP BY 
        event_dt, 
        user_id, 
        event_category, 
        event_action
),

dynamic_refund AS (
    SELECT 
        FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', date)) as event_dt, 
        orderDate as order_date, 
        subParam As user_id, 
        'Order Cancel (App)' As event_category,
        'dynamic' AS event_action, 
        'coin' As event_label,
        SUM(commission) AS Total_Revenue
    FROM `ballosodeuk.external_mart.cpDynamic_cancels` 
    WHERE 
        PARSE_DATE('%Y%m%d', date) BETWEEN "2024-06-19" AND date({end_date})
        AND subParam IS NOT NULL
    GROUP BY 
        event_dt, 
        order_date, 
        user_id, 
        event_category, 
        event_action
),

dynamic AS (
    SELECT 
        o.event_dt, 
        o.user_id, 
        o.event_category, 
        o.event_action, 
        o.event_label, 
        o.Total_Revenue + COALESCE(r.Total_Revenue, 0) as Total_Revenue
    FROM dynamic_order o
    LEFT JOIN dynamic_refund r 
        ON o.user_id = r.user_id 
        AND o.event_dt = r.order_date
),

combined_commerce AS (
    SELECT event_dt, user_id, event_category, event_label, Total_Revenue
    FROM shortcut
    UNION ALL
    SELECT event_dt, user_id, event_category, event_label, Total_Revenue
    FROM dynamic
)

select * from combined_commerce
)