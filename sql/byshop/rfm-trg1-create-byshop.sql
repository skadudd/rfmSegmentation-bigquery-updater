CREATE or Replace TABLE ballosodeuk.ynam.rfm_table_bycommerce AS (
WITH date_vars AS (
    SELECT date({end_date}) as standard_date  -- B: 기준일
)

,order_complete as (
    select event_dt, event_dttm, user_id, airbridge_device_id, platform,transaction_id, event_category, event_label, event_action, price as event_value
    from `ballosodeuk.dw.fact_airbridge_event_order`
    where 
        Date(event_dt) between date("2024-03-01") and date({end_date})
        and event_action not like "^PAY" 
        and event_label != "쇼핑"
        and user_id not like 'IU_%' 
        and user_id is not Null
),

shortcut_order as (
select event_dt, user_id, min(airbridge_device_id) as airbridge_device_id, transaction_id, min(platform) as platform, event_label, event_action, min(event_category) as event_category, sum(Total_Revenue) as Total_Revenue
from( 
    select 
    event_dt, event_dttm, user_id, airbridge_device_id, platform,transaction_id, event_category, event_action, 
    CASE 
        WHEN event_action in ('balso1sa1','balso1sr1','balso2sr1','balso2sa2') then '바로가기'
        WHEN event_action = 'balso1sr2' then '퀴즈쿠팡'
        WHEN event_action = 'balso2sa1' then '챌린지인증쿠팡'
    END AS event_label, 
    round(sum(event_value) * 0.046) as Total_Revenue
    from order_complete
    where event_category = "Order Complete (App)"
    GROUP BY event_dt, event_dttm, user_id, airbridge_device_id,transaction_id, platform, event_category, event_label, event_action
)
group by event_dt, user_id, transaction_id, event_label, event_action
),

shortcut_refund as (
select event_dt, user_id, min(airbridge_device_id) as airbridge_device_id, transaction_id, min(platform) as platform, event_label, event_action, min(event_category) as event_category, sum(Total_Revenue) as Total_Revenue
from( 
    select 
    event_dt, event_dttm, user_id, airbridge_device_id, platform,transaction_id, event_category, event_action, 
    CASE 
        WHEN event_action in ('balso1sa1','balso1sr1','balso2sr1','balso2sa2') then '바로가기'
        WHEN event_action = 'balso1sr2' then '퀴즈쿠팡'
        WHEN event_action = 'balso2sa1' then '챌린지인증쿠팡'
    END AS event_label, 
    round(sum(event_value) * 0.046) * -1 as Total_Revenue
    from order_complete
    where event_category = "Order Cancel (App)"
    GROUP BY event_dt, event_dttm, user_id, airbridge_device_id,transaction_id, platform, event_category, event_label, event_action
)
group by event_dt, user_id, transaction_id, event_label, event_action
),

shortcut as (
select o.event_dt, o.user_id, o.airbridge_device_id, o.transaction_id, o.platform, o.event_label, o.event_action, o.event_category, 
case when o.Total_Revenue + COALESCE(r.Total_Revenue, 0) < 0 then 0 else o.Total_Revenue + COALESCE(r.Total_Revenue, 0) end as Total_Revenue
    -- 구매 <> 환불의 금액이 100% 매칭이 안될 수 있음. 해당 케이스는 0원으로 처리
from shortcut_order o
left join shortcut_refund r on o.user_id = r.user_id and o.transaction_id = r.transaction_id
),

dynamic_order AS (
    SELECT 
    cast(FORMAT_DATE('%Y-%m-%d', DATETIME(date)) as Date) as event_dt, 
    subParam AS user_id, 
    'Order Complete (App)' AS event_category, 
    'dynamic' AS event_action, 
    'coin' As event_label,
    SUM(commission) AS Total_Revenue
    FROM `ballosodeuk.external_mart.cpDynamic_orders` 
    WHERE 
    date between date("2024-03-01") and date_sub(current_date(),interval 1 day)
    AND subParam IS NOT NULL
    GROUP BY 
    event_dt, 
    user_id, 
    event_category, 
    event_action
),

dynamic_refund as (
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
    parse_date('%Y%m%d',date) BETWEEN date("2024-03-01") and date_sub(current_date(),interval 1 day)
    and subParam Is Not Null
group by 
    event_dt, order_date, user_id, event_category, event_action  
),

dynamic as (
select o.event_dt, o.user_id, o.event_category, o.event_action, o.event_label, 
case when o.Total_Revenue + COALESCE(r.Total_Revenue, 0) < 0 then 0 else o.Total_Revenue + COALESCE(r.Total_Revenue, 0) end as Total_Revenue
    -- 구매 <> 환불의 금액이 100% 매칭이 안될 수 있음. 해당 케이스는 0원으로 처리
from dynamic_order o
left join dynamic_refund r on o.user_id = r.user_id and o.event_dt = r.order_date
),

combined_commerce AS (
SELECT event_dt, user_id, event_category, event_label, Total_Revenue
FROM shortcut
UNION ALL
SELECT event_dt, user_id, event_category, event_label, Total_Revenue
FROM dynamic
)

,user_properties_filled as (
select *
from ballosodeuk.dw.dim_airbridge_member
)

,intermediate_commerce_data AS (
SELECT 
    up.user_id,
    up.platform,
    up.join_dt,
    DATE_DIFF(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), DATE(up.join_dt), DAY) AS cum_lifetime,
    max(up.total_accumulate_cash) as total_accumulate_cash,
    max(up.current_cash) as current_cash,
    max(cc.event_dt) as event_dt,
    COUNT(DISTINCT cc.event_dt) as ByCommerce_F,
    SUM(COALESCE(cc.Total_Revenue, 0)) as ByCommerce_M
FROM user_properties_filled up
LEFT JOIN combined_commerce cc ON up.user_id = cc.user_id
WHERE 
    cc.event_dt IS NULL OR 
    DATE(cc.event_dt) <= date({end_date})
GROUP BY 
    up.user_id, up.platform, up.join_dt
)

,commerce_data AS (
SELECT 
    user_id,
    platform,
    join_dt,
    cum_lifetime,
    total_accumulate_cash,
    current_cash,
    CASE 
        WHEN max(event_dt) IS NULL THEN NULL
        ELSE DATE_DIFF(date({end_date}), max(event_dt), DAY)
    END as recency,
    ByCommerce_F as frequency,
    ByCommerce_M as monetary
FROM intermediate_commerce_data
GROUP BY 
    user_id, platform, join_dt, cum_lifetime,
    total_accumulate_cash, current_cash, ByCommerce_F, ByCommerce_M
)

SELECT *, date({end_date}) as snapshot_dt
FROM commerce_data
WHERE monetary > 0
)