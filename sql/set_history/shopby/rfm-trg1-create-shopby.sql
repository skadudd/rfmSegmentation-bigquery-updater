CREATE or Replace TABLE ballosodeuk.ynam.rfm_table_shopby AS (
WITH date_vars AS (
    SELECT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) as yesterday
)

,valid_orders AS (
SELECT 
    order_dt,
    a.member_no,
    b.wk_id as user_id,
    c.join_dt,
    c.total_accumulate_cash,
    c.current_cash,
    c.platform,
    b.gender,
    b.birth_year,
    product_option[SAFE_OFFSET(0)].commission_rate as commission_rate,
    # last_main_pay_amt,
    first_pay_amt, -- 1/20 수정
    product_option[SAFE_OFFSET(0)].order_status_type as order_status_type1
FROM `ballosodeuk.dw.fact_shopby_order` a
LEFT JOIN `ballosodeuk.dw.dim_shopby_member` b 
    ON a.member_no = b.member_no
LEFT JOIN `ballosodeuk.dw.dim_airbridge_member` c
    on b.wk_id = c.user_id
WHERE product_option[SAFE_OFFSET(0)].order_status_type IN (
    'DELIVERY_DONE', 'DELIVERY_PREPARE', 'DELIVERY_ING',
    'BUY_CONFIRM', 'PAY_DONE', 'EXCHANGE_DONE', 'PRODUCT_PREPARE'
) and order_dt between date("2024-10-01") and date({end_date})
),

refund_check AS (
-- 30일 이내 환불 확인
SELECT 
    v.*,
    CASE 
    WHEN r.product_option[SAFE_OFFSET(0)].order_status_type in ('CANCEL DONE', 'RETURN_DONE')
    AND DATE_DIFF(DATE(r.order_dt), v.order_dt, DAY) <= 90 
    AND r.order_dt <= date({end_date})
    THEN TRUE 
    ELSE FALSE 
    END as is_refunded
FROM valid_orders v
LEFT JOIN `ballosodeuk.dw.fact_shopby_order` r
    ON v.member_no = r.member_no
    AND r.product_option[SAFE_OFFSET(0)].order_status_type in ('CANCEL DONE', 'RETURN_DONE')
)
,grp_table as 
(SELECT 
    r.member_no,
    r.user_id,
    MAX(r.platform) as platform,
    MAX(r.join_dt) as join_dt,
    DATE_DIFF(date({end_date}), DATE(MAX(r.join_dt)), DAY) AS cum_lifetime,  -- 수정: CURRENT_DATE -> end_date
    MAX(r.total_accumulate_cash) as total_accumulate_cash,
    MAX(r.current_cash) as current_cash,

    -- recency 계산 수정
    CASE 
        WHEN MAX(CASE WHEN NOT is_refunded THEN order_dt END) IS NULL THEN NULL  -- 구매 이력 없음
        WHEN MAX(CASE WHEN NOT is_refunded THEN order_dt END) > date({end_date}) THEN NULL  -- 미래의 구매
        ELSE DATE_DIFF(date({end_date}), 
                      MAX(CASE WHEN NOT is_refunded THEN order_dt END), 
                      DAY)  -- 마지막 구매일로부터 기준일까지의 기간
    END as recency,

    -- frequency 계산 수정
    COUNT(DISTINCT CASE 
        WHEN NOT is_refunded AND order_dt <= date({end_date})  -- 기준일까지의 구매만 카운트
        THEN order_dt 
    END) as frequency,

    -- monetary 계산 수정
    SUM(CASE 
        WHEN NOT is_refunded AND order_dt <= date({end_date})  -- 기준일까지의 구매 금액만 합산
        # THEN last_main_pay_amt * (commission_rate * 0.01) 
        THEN first_pay_amt -- 1/20 수정 main_pay 의 경우, 0 발생 케이스 확인.
        ELSE 0 
    END) as monetary,

    MAX(r.gender) as gender,
    MAX(r.birth_year) as birth_year,
    AVG(r.commission_rate) as avg_commission_rate,
    COUNT(*) as total_purchases,
    COUNT(CASE WHEN is_refunded THEN 1 END) as total_refunds,
    ROUND(COUNT(CASE WHEN is_refunded THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as refund_rate,
    MAX(CASE 
        WHEN NOT is_refunded AND order_dt <= date({end_date})  -- 기준일까지의 마지막 구매일
        THEN order_dt 
    END) as last_order_dt,
    MIN(order_dt) as first_order_dt -- 1/8 추가
FROM refund_check r
GROUP BY r.member_no, r.user_id)

select *, date({end_date}) as snapshot_dt 
from grp_table

)