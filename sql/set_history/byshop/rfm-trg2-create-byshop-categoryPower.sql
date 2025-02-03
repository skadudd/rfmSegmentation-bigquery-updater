CREATE or Replace TABLE ballosodeuk.ynam.rfm_table_byshop_category_power AS (

WITH order_complete AS (
    SELECT event_dt, event_dttm, user_id, airbridge_device_id, platform,
        transaction_id, event_category, event_label, event_action, name,
        price AS event_value
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
    MIN(airbridge_device_id) AS airbridge_device_id,
    transaction_id,
    MIN(platform) AS platform,
    event_label,
    event_action,
    MIN(name) AS name,
    MIN(event_category) AS event_category,
    SUM(Total_Revenue) AS Total_Revenue
FROM (
    SELECT 
    event_dt, event_dttm, user_id, airbridge_device_id, platform,
    transaction_id, event_category, event_action, name,
    CASE 
        WHEN event_action IN ('balso1sa1','balso1sr1','balso2sr1','balso2sa2') THEN '바로가기'
        WHEN event_action = 'balso1sr2' THEN '퀴즈쿠팡'
        WHEN event_action = 'balso2sa1' THEN '챌린지인증쿠팡'
    END AS event_label,
    ROUND(SUM(event_value) * 0.046) AS Total_Revenue
    FROM order_complete
    WHERE event_category = "Order Complete (App)"
    GROUP BY event_dt, event_dttm, user_id, airbridge_device_id, platform,
            transaction_id, event_category, event_label, event_action, name
)
GROUP BY event_dt, user_id, transaction_id, event_label, event_action
),

shortcut_refund AS (
SELECT 
    event_dt,
    user_id,
    MIN(airbridge_device_id) AS airbridge_device_id,
    transaction_id,
    MIN(platform) AS platform,
    event_label,
    event_action,
    MIN(event_category) AS event_category,
    SUM(Total_Revenue) AS Total_Revenue
FROM (
    SELECT 
    event_dt, event_dttm, user_id, airbridge_device_id, platform, transaction_id,
    event_category, event_action, 
    CASE 
        WHEN event_action IN ('balso1sa1','balso1sr1','balso2sr1','balso2sa2') THEN '바로가기'
        WHEN event_action = 'balso1sr2' THEN '퀴즈쿠팡'
        WHEN event_action = 'balso2sa1' THEN '챌린지인증쿠팡'
    END AS event_label,
    ROUND(SUM(event_value) * 0.046) * -1 AS Total_Revenue
    FROM order_complete
    WHERE event_category = "Order Cancel (App)"
    GROUP BY event_dt, event_dttm, user_id, airbridge_device_id, platform,
            transaction_id, event_category, event_label, event_action
)
GROUP BY event_dt, user_id, transaction_id, event_label, event_action
),

shortcut AS (
SELECT 
    o.event_dt,
    o.user_id,
    o.airbridge_device_id,
    o.transaction_id,
    o.platform,
    o.name,
    o.event_label,
    o.event_action,
    o.event_category,
    CASE WHEN o.Total_Revenue + COALESCE(r.Total_Revenue, 0) < 0
        THEN 0
        ELSE o.Total_Revenue + COALESCE(r.Total_Revenue, 0)
    END AS Total_Revenue
FROM shortcut_order o
LEFT JOIN shortcut_refund r
    ON o.user_id = r.user_id
AND o.transaction_id = r.transaction_id
),

dynamic_order AS (
    SELECT 
    CAST(FORMAT_DATE('%Y-%m-%d', DATETIME(date)) AS DATE) AS event_dt, 
    subParam AS user_id, 
    'Order Complete (App)' AS event_category, 
    'dynamic' AS event_action, 
    'coin' AS event_label,
    MIN(productName) AS name,
    SUM(commission) AS Total_Revenue
    FROM `ballosodeuk.external_mart.cpDynamic_orders`
    WHERE 
    date BETWEEN date("2024-03-01") AND date({end_date})
    AND subParam IS NOT NULL
    GROUP BY event_dt, user_id, event_category, event_action
),

dynamic_refund AS (
SELECT 
    FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', date)) AS event_dt,
    orderDate AS order_date,
    subParam AS user_id, 
    'Order Cancel (App)' AS event_category,
    'dynamic' AS event_action, 
    'coin' AS event_label,
    SUM(commission) AS Total_Revenue
FROM `ballosodeuk.external_mart.cpDynamic_cancels`
WHERE 
    PARSE_DATE('%Y%m%d', date) BETWEEN date("2024-03-01") AND date({end_date})
    AND subParam IS NOT NULL
GROUP BY event_dt, order_date, user_id, event_category, event_action
),

dynamic AS (
SELECT 
    o.event_dt,
    o.user_id,
    o.event_category,
    o.event_action,
    o.event_label,
    o.name,
    CASE WHEN o.Total_Revenue + COALESCE(r.Total_Revenue, 0) < 0
        THEN 0
        ELSE o.Total_Revenue + COALESCE(r.Total_Revenue, 0)
    END AS Total_Revenue
FROM dynamic_order o
LEFT JOIN dynamic_refund r
    ON o.user_id = r.user_id
AND o.event_dt = r.order_date
),

combined_commerce AS (
SELECT event_dt, user_id, event_category, event_label, name, Total_Revenue
    FROM shortcut
UNION ALL
SELECT event_dt, user_id, event_category, event_label, name, Total_Revenue
    FROM dynamic
),

/* ------ 상품명 vs 카테고리 매핑 ------ */
category_raw AS (
SELECT 
    c.*,
    k.category1,
    k.category2
FROM combined_commerce c
LEFT JOIN ballosodeuk.external_mart.cpProduct_keyword k
    ON k.name = c.name
),

/* ------ depth 카테고리 정제 ------ */
depth1_raw AS (
SELECT 
    c.*,
    REPLACE(category1, '/', '-') AS depth1
FROM category_raw c
),
depth2_raw AS (
SELECT
    c.*,
    REPLACE(category2, '/', '-') AS depth2,
    REPLACE(category1, '/', '-') AS depth1
FROM category_raw c
),

-------------------------------------------------------------------------------
-- [B-1] Depth1만 처리 (Top3)
-------------------------------------------------------------------------------
user_category_stats_dept1 AS (
SELECT 
    user_id,
    depth1,
    DATE_DIFF(MAX(event_dt), CURRENT_DATE(), DAY) AS latest_order_dt,
    COUNT(DISTINCT event_dt) AS order_count
FROM depth1_raw
WHERE category1 IS NOT NULL
    AND TRIM(category1) != ''
    AND TRIM(category1) != 'None'
GROUP BY user_id, depth1
),

raw_weight_cte_depth1 AS (
SELECT
    user_id,
    depth1,
    ABS(latest_order_dt) AS days,
    order_count,
    SUM(order_count) OVER (PARTITION BY user_id) AS total_order_count
FROM user_category_stats_dept1
),

score_cte_depth1 AS (
SELECT
    user_id,
    depth1,
    days,
    order_count,
    SAFE_DIVIDE(order_count, total_order_count) AS freq_weight,
    EXP(-0.1 * days) AS recency_weight,
    0.6 * SAFE_DIVIDE(order_count, total_order_count)
    + 0.4 * EXP(-0.1 * days) AS final_score
FROM raw_weight_cte_depth1
),

final_calc_depth1 AS (
SELECT
    user_id,
    depth1,
    final_score,
    SUM(final_score) OVER (PARTITION BY user_id) AS sum_score
FROM score_cte_depth1
),

result_depth1 AS (
SELECT
    user_id,
    depth1,
    final_score,
    ROUND(SAFE_DIVIDE(final_score, sum_score), 2) AS interest_ratio
FROM final_calc_depth1
),

rank_table_depth1 AS (
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY interest_ratio DESC) AS ranking
FROM result_depth1
),

pivot_table_depth1 AS (
SELECT 
    user_id,
    MAX(CASE WHEN ranking = 1 THEN depth1 END) AS ranking_1_1,
    MAX(CASE WHEN ranking = 2 THEN depth1 END) AS ranking_1_2,
    MAX(CASE WHEN ranking = 3 THEN depth1 END) AS ranking_1_3,

    MAX(CASE WHEN ranking = 1 THEN interest_ratio END) AS power_1_1,
    MAX(CASE WHEN ranking = 2 THEN interest_ratio END) AS power_1_2,
    MAX(CASE WHEN ranking = 3 THEN interest_ratio END) AS power_1_3
FROM rank_table_depth1
WHERE ranking <= 3
GROUP BY user_id
),

-------------------------------------------------------------------------------
-- [B-2] Depth1(Depth2) 처리 (Top6)
-------------------------------------------------------------------------------
-- 1) depth2가 NULL이면 '없음'으로 치환 → depth1( depth2 ) 문자열로 합치기
user_category_stats_dept1plus AS (
SELECT 
    user_id,
    CONCAT(
    depth1,
    '(',
    COALESCE(NULLIF(TRIM(depth2), ''), '없음'),
    ')'
    ) AS depth1plus,
    DATE_DIFF(MAX(event_dt), CURRENT_DATE(), DAY) AS latest_order_dt,
    COUNT(DISTINCT event_dt) AS order_count
FROM depth2_raw
WHERE category1 IS NOT NULL
    AND TRIM(category1) != ''
    AND TRIM(category1) != 'None'
    -- depth2가 비어도 상관없음, 위에서 COALESCE( '없음' ) 처리
GROUP BY user_id, depth1, depth2
),

raw_weight_cte_depth1plus AS (
SELECT
    user_id,
    depth1plus,
    ABS(latest_order_dt) AS days,
    order_count,
    SUM(order_count) OVER (PARTITION BY user_id) AS total_order_count
FROM user_category_stats_dept1plus
),

score_cte_depth1plus AS (
SELECT
    user_id,
    depth1plus,
    days,
    order_count,
    SAFE_DIVIDE(order_count, total_order_count) AS freq_weight,
    EXP(-0.1 * days) AS recency_weight,
    0.6 * SAFE_DIVIDE(order_count, total_order_count)
    + 0.4 * EXP(-0.1 * days) AS final_score
FROM raw_weight_cte_depth1plus
),

final_calc_depth1plus AS (
SELECT
    user_id,
    depth1plus,
    final_score,
    SUM(final_score) OVER (PARTITION BY user_id) AS sum_score
FROM score_cte_depth1plus
),

result_depth1plus AS (
SELECT
    user_id,
    depth1plus,
    final_score,
    ROUND(SAFE_DIVIDE(final_score, sum_score), 2) AS interest_ratio
FROM final_calc_depth1plus
),

rank_table_depth1plus AS (
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY interest_ratio DESC) AS ranking
FROM result_depth1plus
),

-- 2) Top6 Pivot
pivot_table_depth1plus AS (
SELECT 
    user_id,

    MAX(CASE WHEN ranking=1 THEN depth1plus END) AS ranking_2_1,
    MAX(CASE WHEN ranking=2 THEN depth1plus END) AS ranking_2_2,
    MAX(CASE WHEN ranking=3 THEN depth1plus END) AS ranking_2_3,
    MAX(CASE WHEN ranking=4 THEN depth1plus END) AS ranking_2_4,
    MAX(CASE WHEN ranking=5 THEN depth1plus END) AS ranking_2_5,
    MAX(CASE WHEN ranking=6 THEN depth1plus END) AS ranking_2_6,

    MAX(CASE WHEN ranking=1 THEN interest_ratio END) AS power_2_1,
    MAX(CASE WHEN ranking=2 THEN interest_ratio END) AS power_2_2,
    MAX(CASE WHEN ranking=3 THEN interest_ratio END) AS power_2_3,
    MAX(CASE WHEN ranking=4 THEN interest_ratio END) AS power_2_4,
    MAX(CASE WHEN ranking=5 THEN interest_ratio END) AS power_2_5,
    MAX(CASE WHEN ranking=6 THEN interest_ratio END) AS power_2_6

FROM rank_table_depth1plus
WHERE ranking <= 6
GROUP BY user_id
),

-------------------------------------------------------------------------------
-- [C] 최종 JOIN: depth1 전용 Top3 + depth1(depth2) Top6
-------------------------------------------------------------------------------
final_join AS (
SELECT 
    p1.user_id,

    -- depth1 전용
    p1.ranking_1_1, p1.ranking_1_2, p1.ranking_1_3,
    p1.power_1_1,   p1.power_1_2,   p1.power_1_3,

    -- depth1(depth2) 전용 (Top6)
    p2.ranking_2_1, p2.ranking_2_2, p2.ranking_2_3,
    p2.ranking_2_4, p2.ranking_2_5, p2.ranking_2_6,

    p2.power_2_1,   p2.power_2_2,   p2.power_2_3,
    p2.power_2_4,   p2.power_2_5,   p2.power_2_6

FROM pivot_table_depth1 p1
LEFT JOIN pivot_table_depth1plus p2
    ON p1.user_id = p2.user_id
)

SELECT *
FROM final_join
ORDER BY user_id


)