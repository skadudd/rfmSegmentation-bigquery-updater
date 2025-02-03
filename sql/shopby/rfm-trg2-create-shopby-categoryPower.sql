CREATE OR REPLACE TABLE ballosodeuk.ynam.rfm_table_shopby_category_power AS (

WITH
----------------------------------------------------------------------
-- 1) [order_counts1] : Depth1 기준 (기존 로직)
----------------------------------------------------------------------
order_counts1 AS (
SELECT 
    member_no,
    depth1_category_no,
    MAX(depth1_category_name) AS depth1_category_name,
    MAX(order_dt) AS latest_order_dt,
    COUNT(DISTINCT order_dt) AS order_count
FROM (
    SELECT 
    order_dt, member_no, category_no,
    depth1_category_no,
    REPLACE(depth1_category_name, '/', '-') AS depth1_category_name
    FROM ballosodeuk.dw.fact_shopby_order
    LEFT JOIN ballosodeuk.dw.dim_shopby_product_category b
    ON b.depth4_category_no = category_no
        OR b.depth3_category_no = category_no
    WHERE order_dt between date("2024-10-01") and date({end_date})
) a
GROUP BY member_no, depth1_category_no
),

----------------------------------------------------------------------
-- 2) [order_counts2plus] : Depth1(Depth2) 기준 (새로운 로직)
--     depth2가 NULL이면 (없음)으로 치환
----------------------------------------------------------------------
order_counts2plus AS (
SELECT
    member_no,
    -- "depth1_category_name(depth2_category_name or 없음)" 형태로 합침
    CONCAT(
    REPLACE(depth1_category_name, '/', '-'),
    '(',
    CASE 
        WHEN depth2_category_name IS NULL 
            OR TRIM(REPLACE(depth2_category_name, '/', '-')) = ''
        THEN '없음'
        ELSE REPLACE(depth2_category_name, '/', '-')
    END,
    ')'
    ) AS depth1plus,
    MAX(order_dt) AS latest_order_dt,
    COUNT(DISTINCT order_dt) AS order_count
FROM (
    SELECT
    order_dt, member_no, category_no,
    REPLACE(depth1_category_name, '/', '-') AS depth1_category_name,
    REPLACE(depth2_category_name, '/', '-') AS depth2_category_name
    FROM ballosodeuk.dw.fact_shopby_order
    LEFT JOIN ballosodeuk.dw.dim_shopby_product_category b
    ON b.depth4_category_no = category_no
        OR b.depth3_category_no = category_no
    WHERE order_dt between date("2024-10-01") and date({end_date})
) a
WHERE depth1_category_name IS NOT NULL
    AND TRIM(depth1_category_name) != ''
    AND TRIM(depth1_category_name) != 'None'
GROUP BY member_no, depth1_category_name, depth2_category_name
),

----------------------------------------------------------------------
-- [A] Depth1 파트 (Top3)
----------------------------------------------------------------------
depth1_score AS (
SELECT
    member_no,
    depth1_category_no,
    depth1_category_name,
    order_count,
    ABS(DATE_DIFF(CURRENT_DATE(), latest_order_dt, DAY)) AS days,
    SUM(order_count) OVER (PARTITION BY member_no) AS total_order_count
FROM order_counts1
),
depth1_calc AS (
SELECT
    member_no,
    depth1_category_no,
    depth1_category_name,
    0.6 * SAFE_DIVIDE(order_count, total_order_count)
    + 0.4 * EXP(-0.1 * days) AS final_score,
    SUM(order_count) OVER (PARTITION BY member_no) AS total_order_count
FROM (
    SELECT
    member_no,
    depth1_category_no,
    depth1_category_name,
    order_count,
    days
    , total_order_count
    , EXP(-0.1 * days) AS recency_weight
    FROM depth1_score
)
),
depth1_final AS (
SELECT
    member_no,
    depth1_category_no,
    depth1_category_name,
    final_score,
    SUM(final_score) OVER (PARTITION BY member_no) AS sum_score
FROM depth1_calc
),
depth1_result AS (
SELECT
    member_no,
    depth1_category_name,
    ROUND(SAFE_DIVIDE(final_score, sum_score), 2) AS percentage
FROM depth1_final
),
rank_depth_1 AS (
SELECT
    d.*,
    ROW_NUMBER() OVER (PARTITION BY member_no ORDER BY percentage DESC) AS ranking
FROM depth1_result d
),
pivot_depth_1 AS (
SELECT
    member_no,
    MAX(CASE WHEN ranking=1 THEN depth1_category_name END) AS ranking_1_1,
    MAX(CASE WHEN ranking=2 THEN depth1_category_name END) AS ranking_1_2,
    MAX(CASE WHEN ranking=3 THEN depth1_category_name END) AS ranking_1_3,

    MAX(CASE WHEN ranking=1 THEN percentage END) AS power_1_1,
    MAX(CASE WHEN ranking=2 THEN percentage END) AS power_1_2,
    MAX(CASE WHEN ranking=3 THEN percentage END) AS power_1_3
FROM rank_depth_1
WHERE ranking <= 3
GROUP BY member_no
),

----------------------------------------------------------------------
-- [B] Depth1(Depth2) 파트 (Top6)
----------------------------------------------------------------------
score_plus AS (
SELECT
    member_no,
    depth1plus,
    ABS(DATE_DIFF(CURRENT_DATE(), latest_order_dt, DAY)) AS days,
    order_count,
    SUM(order_count) OVER (PARTITION BY member_no) AS total_order_count
FROM order_counts2plus
),
-- 1) calc_plus: final_score만 계산
calc_plus AS (
SELECT
    member_no,
    depth1plus,
    -- 최종 점수: 0.6*freq + 0.4*recency
    0.6 * SAFE_DIVIDE(order_count, total_order_count)
    + 0.4 * EXP(-0.1 * days) AS final_score
FROM (
    SELECT
    member_no,
    depth1plus,
    order_count,
    total_order_count,
    days
    -- recency_weight = EXP(-0.1 * days) 계산해도 되지만, 
    -- 여기서 바로 0.4 * exp(-0.1 * days) 해도 OK
    FROM score_plus
)
),

-- 2) final_plus: calc_plus 결과에서 sum_score 구하기
final_plus AS (
SELECT
    member_no,
    depth1plus,
    final_score,
    -- user_id별 final_score 합
    SUM(final_score) OVER (PARTITION BY member_no) AS sum_score
FROM calc_plus
)

,result_plus AS (
SELECT
    member_no,
    depth1plus,
    ROUND(SAFE_DIVIDE(final_score, sum_score), 2) AS percentage
FROM final_plus
),
rank_plus AS (
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY member_no ORDER BY percentage DESC) AS ranking
FROM result_plus
),
pivot_depth1plus AS (
SELECT
    member_no,
    MAX(CASE WHEN ranking=1 THEN depth1plus END) AS ranking_2_1,
    MAX(CASE WHEN ranking=2 THEN depth1plus END) AS ranking_2_2,
    MAX(CASE WHEN ranking=3 THEN depth1plus END) AS ranking_2_3,
    MAX(CASE WHEN ranking=4 THEN depth1plus END) AS ranking_2_4,
    MAX(CASE WHEN ranking=5 THEN depth1plus END) AS ranking_2_5,
    MAX(CASE WHEN ranking=6 THEN depth1plus END) AS ranking_2_6,

    MAX(CASE WHEN ranking=1 THEN percentage END) AS power_2_1,
    MAX(CASE WHEN ranking=2 THEN percentage END) AS power_2_2,
    MAX(CASE WHEN ranking=3 THEN percentage END) AS power_2_3,
    MAX(CASE WHEN ranking=4 THEN percentage END) AS power_2_4,
    MAX(CASE WHEN ranking=5 THEN percentage END) AS power_2_5,
    MAX(CASE WHEN ranking=6 THEN percentage END) AS power_2_6
FROM rank_plus
WHERE ranking <= 6
GROUP BY member_no
),

----------------------------------------------------------------------
-- [C] 최종 Join (Depth1 & Depth1(Depth2)) + 멤버 매핑
----------------------------------------------------------------------
final_join AS (
SELECT
    p1.member_no,
    
    /* Depth1 전용 Top3 */
    p1.ranking_1_1, p1.ranking_1_2, p1.ranking_1_3,
    p1.power_1_1,   p1.power_1_2,   p1.power_1_3,
    
    /* Depth1(Depth2) 전용 Top6 */
    p2.ranking_2_1, p2.ranking_2_2, p2.ranking_2_3,
    p2.ranking_2_4, p2.ranking_2_5, p2.ranking_2_6,
    
    p2.power_2_1,   p2.power_2_2,   p2.power_2_3,
    p2.power_2_4,   p2.power_2_5,   p2.power_2_6
FROM pivot_depth_1 p1
LEFT JOIN pivot_depth1plus p2
    ON p1.member_no = p2.member_no
)

SELECT
m.wk_id AS user_id,
f.* EXCEPT(member_no)
FROM final_join f
LEFT JOIN ballosodeuk.dw.dim_shopby_member m
ON f.member_no = m.member_no
ORDER BY user_id

)