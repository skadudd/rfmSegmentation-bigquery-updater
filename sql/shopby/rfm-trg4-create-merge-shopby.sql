create or replace table ballosodeuk.ynam.rfm_table_merged_shopby as (

WITH
-------------------------------------------------------------------------------
-- 1) 쇼핑몰 기준: frequency >= 1
shopby_base AS (
SELECT DISTINCT
    user_id,
    member_no,
    platform,
    join_dt,
    cum_lifetime,
    gender,
    birth_year,
    total_accumulate_cash,
    recency,                     -- commerce_r
    frequency AS commerce_f,     -- 쇼핑몰 구매 빈도
    monetary  AS commerce_m,      -- 쇼핑몰 구매 금액
    first_order_dt, -- 1/8 추가
    refund_rate -- 1/8 추가

FROM ballosodeuk.ynam.rfm_table_shopby
WHERE monetary >= 1 -- 1/9 수정 (as is frequency)
),

-------------------------------------------------------------------------------
-- 2) 쿠팡 RFM 병합 (LEFT JOIN)
shopby_left_bycommerce AS (
SELECT
    sb.user_id,sb.member_no,
    COALESCE(sb.platform, b.platform) AS platform,
    COALESCE(sb.join_dt, b.join_dt)   AS join_dt,
    COALESCE(sb.cum_lifetime, b.cum_lifetime) AS cum_lifetime,
    sb.gender,
    sb.birth_year,
    COALESCE(sb.total_accumulate_cash, b.total_accumulate_cash) AS total_accumulate_cash,
    sb.first_order_dt,
    sb.refund_rate,
    
    sb.recency  AS recency,      -- 쇼핑몰 recency
    sb.commerce_f AS commerce_f, -- 쇼핑몰 freq
    sb.commerce_m AS commerce_m, -- 쇼핑몰 monetary
    
    b.recency    AS byrecency,   -- 쿠팡 recency
    b.frequency  AS bycommerce_f,
    b.monetary   AS bycommerce_m
FROM shopby_base sb
LEFT JOIN ballosodeuk.ynam.rfm_table_bycommerce b
    ON sb.user_id = b.user_id
),

-------------------------------------------------------------------------------
-- 3) 오퍼월 RFM 병합 (LEFT JOIN)
merged_rfm AS (
SELECT 
    w.user_id,
    w.member_no,
    w.platform,
    w.join_dt,
    w.cum_lifetime,
    w.gender,
    w.birth_year,
    w.total_accumulate_cash,
    w.first_order_dt,
    w.refund_rate,

    w.recency,
    w.commerce_f,
    w.commerce_m,

    w.byrecency,
    w.bycommerce_f,
    w.bycommerce_m,

    /* noncommerce 병합 */
    n.recency   AS nonrecency,
    n.frequency AS noncommerce_f,
    n.monetary  AS noncommerce_m
    
FROM shopby_left_bycommerce w
LEFT JOIN ballosodeuk.ynam.rfm_table_noncommerce n
    ON w.user_id = n.user_id
),

-------------------------------------------------------------------------------
-- 4) 쇼지, 서바이벌, 카테고리 파워 등 부가 정보
fill_shoji_properties AS (
SELECT *
from ballosodeuk.ynam.rfm_table_shopby_shoji_prop
),

fill_shopby_churn_properties AS (
SELECT *
FROM ballosodeuk.ynam.rfm_table_shopby_survive_prop
),

fill_byshop_churn_properties AS (
SELECT *
FROM ballosodeuk.ynam.rfm_table_byshop_survive_prop
),

shopby_categorypower AS (
SELECT *
FROM ballosodeuk.ynam.rfm_table_shopby_category_power
),

byshop_categorypower AS (
SELECT *
FROM ballosodeuk.ynam.rfm_table_byshop_category_power
),

-------------------------------------------------------------------------------
-- 5) 유저 프로퍼티 최종 필링
fill_userproperties AS (
SELECT 
    /* merged_rfm */ a.* EXCEPT(gender, birth_year, join_dt, platform, total_accumulate_cash)
    
    -- 기본 유저 정보
    ,CASE WHEN a.gender     IS NULL THEN b.gender     ELSE a.gender     END AS gender
    ,CASE WHEN a.birth_year IS NULL THEN b.birth_year ELSE a.birth_year END AS birth_year
    ,CASE WHEN a.join_dt    IS NULL THEN b.join_dt    ELSE a.join_dt    END AS join_dt
    ,CASE WHEN a.platform   IS NULL THEN b.platform   ELSE a.platform   END AS platform
    ,CASE WHEN a.total_accumulate_cash IS NULL THEN b.total_accumulate_cash 
        ELSE a.total_accumulate_cash END AS total_accumulate_cash
    
    ,b.terms_agree_yn
    
    -- 쇼지
    ,c.pre_cash
    ,c.current_cash
    ,c.current_shoji
    ,c.earn
    ,c.spend
    ,c.exchange
    ,c.exchange_cash_rate
    ,c.burnt
    
    -- shopby churn
    ,d.days_since_last_purchase      AS last_purchase_shop
    ,d.current_trailing_term         AS current_trailing_term_shop
    ,d.prev_trailing_term            AS prev_trailing_term_shop
    ,d.cycle_stddev                  AS cycle_stddev_shop
    ,d.survival_prob                 AS suvival_prob_shop
    ,d.predicted_survival_time       AS predicted_survival_time_shop
    ,d.risk_level                    AS risk_level_shop
    ,d.cycle_length                  AS cycle_length_shop
    
    -- byshop churn
    ,e.days_since_last_purchase      AS last_purchase_byshop
    ,e.current_trailing_term         AS current_trailing_term_byshop
    ,e.prev_trailing_term           AS prev_trailing_term_byshop
    ,e.cycle_stddev                  AS cycle_stddev_byshop
    ,e.survival_prob                 AS suvival_prob_byshop
    ,e.predicted_survival_time       AS predicted_survival_time_byshop
    ,e.risk_level                    AS risk_level_byshop
    ,e.cycle_length                  AS cycle_length_byshop
    
    -- shopby 카테고리 파워
    ,f.ranking_1_1      AS ranking_1_1_sp
    ,f.ranking_1_2      AS ranking_1_2_sp
    ,f.ranking_1_3      AS ranking_1_3_sp
    ,f.power_1_1        AS power_1_1_sp
    ,f.power_1_2        AS power_1_2_sp
    ,f.power_1_3        AS power_1_3_sp
    ,f.ranking_2_1      AS ranking_2_1_sp
    ,f.ranking_2_2      AS ranking_2_2_sp
    ,f.ranking_2_3      AS ranking_2_3_sp
    ,f.ranking_2_4      AS ranking_2_4_sp
    ,f.ranking_2_5      AS ranking_2_5_sp
    ,f.ranking_2_6      AS ranking_2_6_sp
    ,f.power_2_1        AS power_2_1_sp
    ,f.power_2_2        AS power_2_2_sp
    ,f.power_2_3        AS power_2_3_sp
    ,f.power_2_4        AS power_2_4_sp
    ,f.power_2_5        AS power_2_5_sp
    ,f.power_2_6        AS power_2_6_sp
    
    -- byshop 카테고리 파워
    ,g.ranking_1_1      AS ranking_1_1_bs
    ,g.ranking_1_2      AS ranking_1_2_bs
    ,g.ranking_1_3      AS ranking_1_3_bs
    ,g.power_1_1        AS power_1_1_bs
    ,g.power_1_2        AS power_1_2_bs
    ,g.power_1_3        AS power_1_3_bs
    ,g.ranking_2_1      AS ranking_2_1_bs
    ,g.ranking_2_2      AS ranking_2_2_bs
    ,g.ranking_2_3      AS ranking_2_3_bs
    ,g.ranking_2_4      AS ranking_2_4_bs
    ,g.ranking_2_5      AS ranking_2_5_bs
    ,g.ranking_2_6      AS ranking_2_6_bs
    ,g.power_2_1        AS power_2_1_bs
    ,g.power_2_2        AS power_2_2_bs
    ,g.power_2_3        AS power_2_3_bs
    ,g.power_2_4        AS power_2_4_bs
    ,g.power_2_5        AS power_2_5_bs
    ,g.power_2_6        AS power_2_6_bs
    
FROM merged_rfm a

LEFT JOIN (
    SELECT 
    inner_a.user_id,
    inner_a.join_dt,
    inner_a.platform,
    inner_a.current_cash, 
    inner_a.total_accumulate_cash, 
    inner_a.terms_agree_yn,
    inner_b.gender,
    inner_b.birth_year
    FROM ballosodeuk.dw.dim_airbridge_member inner_a
    LEFT JOIN ballosodeuk.dw.dim_shopby_member inner_b
    ON inner_a.user_id = inner_b.wk_id
) b
    ON a.user_id = b.user_id

LEFT JOIN fill_shoji_properties c
    ON a.user_id = c.user_id

LEFT JOIN fill_shopby_churn_properties d
    ON a.user_id = d.user_id

LEFT JOIN fill_byshop_churn_properties e
    ON a.user_id = e.user_id

LEFT JOIN shopby_categorypower f
    ON a.user_id = f.user_id

LEFT JOIN byshop_categorypower g
    ON a.user_id = g.user_id
),

-------------------------------------------------------------------------------
-- 6) 마지막 그룹바이 + 필요한 로직
source_table AS (
SELECT 
    user_id,
    MAX(member_no)                 AS member_no,
    MAX(platform)                 AS platform,
    MAX(join_dt)                  AS join_dt,
    MAX(cum_lifetime)            AS cum_lifetime,
    MAX(gender)                  AS gender,
    MAX(birth_year)              AS birth_year,
    MAX(terms_agree_yn)          AS terms_agree_yn,
    MAX(first_order_dt)               AS first_order_dt,
    MAX(refund_rate)               AS refund_rate,

    -- 재산
    MAX(COALESCE(cast (total_accumulate_cash as int64) ,0))    AS total_accumulate_cash,
    sum(COALESCE(earn,0)) + sum(coalesce(exchange,0)) AS total_accumulate_shoji,

    max(coalesce(pre_cash,0)) as pre_cash,
    max(coalesce(current_cash,0)) as current_cash,
    max(coalesce(current_shoji,0)) as current_shoji,
    max(coalesce(earn,0)) as earn,
    max(coalesce(spend,0)) as spend,
    max(coalesce(exchange,0)) as exchange,
    max(coalesce(exchange_cash_rate,0)) as exchange_cash_rate,
    max(coalesce(burnt,0)) as burnt,

    --  유저의 쇼핑 프로퍼티
    -- --  쇼핑
    max (last_purchase_shop) as last_purchase_shop,
    max(current_trailing_term_shop) as current_trailing_term_shop, 
    max(prev_trailing_term_shop) as prev_trailing_term_shop,
    max(cycle_stddev_shop) as cycle_stddev_shop,
    max(suvival_prob_shop) as suvival_prob_shop,
    max(predicted_survival_time_shop) as predicted_survival_time_shop,
    max(risk_level_shop) as risk_level_shop,
    max(cycle_length_shop) as cycle_length_shop,

    -- --  쿠팡
    max (last_purchase_byshop) as last_purchase_byshop,
    max(current_trailing_term_byshop) as current_trailing_term_byshop, 
    max(prev_trailing_term_byshop) as prev_trailing_term_byshop,
    max(cycle_stddev_byshop) as cycle_stddev_byshop,
    max(suvival_prob_byshop) as suvival_prob_byshop,
    max(predicted_survival_time_byshop) as predicted_survival_time_byshop,
    max(risk_level_byshop) as risk_level_byshop,
    max(cycle_length_byshop) as cycle_length_byshop,

    -- 쇼핑몰 RFM
    COALESCE(MIN(recency), 99999) AS r_shop,
    MAX(COALESCE(CAST(commerce_f AS INT64), 0)) AS f_shop,
    MAX(COALESCE(CAST(commerce_m AS INT64), 0)) AS m_shop,

    -- 쿠팡 RFM
    COALESCE(MIN(byrecency), 99999)    AS r_byshop,
    MAX(COALESCE(CAST(bycommerce_f AS INT64), 0)) AS f_byshop,
    MAX(COALESCE(CAST(bycommerce_m AS INT64), 0)) AS m_byshop,

    -- 오퍼월 RFM
    COALESCE(MIN(nonrecency), 99999)   AS r_noncommerce,
    MAX(COALESCE(CAST(noncommerce_f AS INT64), 0)) AS f_noncommerce,
    MAX(COALESCE(CAST(noncommerce_m AS INT64), 0)) AS m_noncommerce,

    -- 3개 합
    COALESCE(MAX(noncommerce_m),0) + COALESCE(MAX(commerce_m),0) + COALESCE(MAX(bycommerce_m),0) AS m_total,
    COALESCE(MAX(noncommerce_f),0) + COALESCE(MAX(commerce_f),0) + COALESCE(MAX(bycommerce_f),0) AS f_total,

    -- 카테고리 파워(쇼핑몰)
    MAX(ranking_1_1_sp) AS ranking_1_1_sp,
    MAX(ranking_1_2_sp) AS ranking_1_2_sp,
    MAX(ranking_1_3_sp) AS ranking_1_3_sp,
    MAX(power_1_1_sp)   AS power_1_1_sp,
    MAX(power_1_2_sp)   AS power_1_2_sp,
    MAX(power_1_3_sp)   AS power_1_3_sp,
    MAX(ranking_2_1_sp) AS ranking_2_1_sp,
    MAX(ranking_2_2_sp) AS ranking_2_2_sp,
    MAX(ranking_2_3_sp) AS ranking_2_3_sp,
    MAX(ranking_2_4_sp) AS ranking_2_4_sp,
    MAX(ranking_2_5_sp) AS ranking_2_5_sp,
    MAX(ranking_2_6_sp) AS ranking_2_6_sp,
    MAX(power_2_1_sp)   AS power_2_1_sp,
    MAX(power_2_2_sp)   AS power_2_2_sp,
    MAX(power_2_3_sp)   AS power_2_3_sp,
    MAX(power_2_4_sp)   AS power_2_4_sp,
    MAX(power_2_5_sp)   AS power_2_5_sp,
    MAX(power_2_6_sp)   AS power_2_6_sp,

    -- 카테고리 파워(쿠팡)
    MAX(ranking_1_1_bs) AS ranking_1_1_bs,
    MAX(ranking_1_2_bs) AS ranking_1_2_bs,
    MAX(ranking_1_3_bs) AS ranking_1_3_bs,
    MAX(power_1_1_bs)   AS power_1_1_bs,
    MAX(power_1_2_bs)   AS power_1_2_bs,
    MAX(power_1_3_bs)   AS power_1_3_bs,
    MAX(ranking_2_1_bs) AS ranking_2_1_bs,
    MAX(ranking_2_2_bs) AS ranking_2_2_bs,
    MAX(ranking_2_3_bs) AS ranking_2_3_bs,
    MAX(ranking_2_4_bs) AS ranking_2_4_bs,
    MAX(ranking_2_5_bs) AS ranking_2_5_bs,
    MAX(ranking_2_6_bs) AS ranking_2_6_bs,
    MAX(power_2_1_bs)   AS power_2_1_bs,
    MAX(power_2_2_bs)   AS power_2_2_bs,
    MAX(power_2_3_bs)   AS power_2_3_bs,
    MAX(power_2_4_bs)   AS power_2_4_bs,
    MAX(power_2_5_bs)   AS power_2_5_bs,
    MAX(power_2_6_bs)   AS power_2_6_bs

FROM fill_userproperties
GROUP BY user_id
)

SELECT 
CASE 
    WHEN f_shop > 0 THEN "shopping" 
    ELSE "non-shopping" 
END AS tgt,
*,
{end_date} AS snapshot_dt
FROM source_table
)

