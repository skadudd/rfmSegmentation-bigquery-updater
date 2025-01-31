create or replace table ballosodeuk.ynam.rfm_table_shopby_survive_prop as (
WITH base_data AS (
SELECT 
    b.wk_id as user_id,
    a.order_dt,
    b.gender,
    b.birth_year,
    row_number() OVER (PARTITION BY b.wk_id ORDER BY a.order_dt DESC) as recency_rank,
    row_number() OVER (PARTITION BY b.wk_id ORDER BY a.order_dt ASC) as purchase_rank
FROM (
    SELECT member_no, order_dt
    FROM ballosodeuk.dw.fact_shopby_order
    WHERE order_dt between date("2024-10-01") and date({end_date}) --  for문 사유로 추가 1/3
    GROUP BY member_no, order_dt
) a
LEFT JOIN ballosodeuk.dw.dim_shopby_member b 
    ON a.member_no = b.member_no
),

recent_purchase as (
SELECT 
    user_id,
    order_dt as latest_order_dt,
    date_diff({end_date_plus}, order_dt, day) as days_since_last_purchase
FROM base_data
WHERE recency_rank = 1
),

purchase_intervals AS (
SELECT 
    user_id,
    order_dt,
    purchase_rank,
    LEAD(order_dt) OVER (PARTITION BY user_id ORDER BY order_dt) as next_order_date,
    date_diff(
    LEAD(order_dt) OVER (PARTITION BY user_id ORDER BY order_dt),
    order_dt,
    day
    ) as days_between_orders
FROM base_data
WHERE purchase_rank <= 15
),

user_stats as (
SELECT 
    user_id,
    stddev(days_between_orders) as cycle_stddev
FROM purchase_intervals
WHERE days_between_orders is not null
GROUP BY user_id
),

current_trailing AS (
SELECT 
    user_id,
    round(avg(days_between_orders), 1) as current_trailing_term,
    count(*) as current_count
FROM purchase_intervals
WHERE days_between_orders IS NOT NULL
AND purchase_rank >= 1 
AND purchase_rank <= 3
GROUP BY user_id
),

prev_trailing AS (
SELECT 
    user_id,
    round(avg(days_between_orders), 1) as prev_trailing_term,
    count(*) as prev_count
FROM purchase_intervals
WHERE days_between_orders IS NOT NULL
AND purchase_rank >= 2
AND purchase_rank <= 4
GROUP BY user_id
),

survival_base AS (
SELECT 
    c.user_id,
    r.days_since_last_purchase,
    c.current_trailing_term,
    p.prev_trailing_term,
    s.cycle_stddev,
    ROUND(((c.current_trailing_term - p.prev_trailing_term) / 
    NULLIF(p.prev_trailing_term, 0)) * 100, 1) as cycle_change_rate,
    ROUND((s.cycle_stddev / NULLIF(c.current_trailing_term, 0)) * 100, 1) as cycle_variation_rate,
    b.gender,
    CAST(FLOOR((EXTRACT(YEAR FROM date({end_date_plus})) - SAFE_CAST(b.birth_year AS INT64)) / 10) * 10 AS STRING) as age_group
FROM current_trailing c
LEFT JOIN prev_trailing p ON c.user_id = p.user_id
LEFT JOIN user_stats s ON c.user_id = s.user_id
LEFT JOIN recent_purchase r ON c.user_id = r.user_id
LEFT JOIN base_data b ON c.user_id = b.user_id AND b.recency_rank = 1
),

term_stats AS (
SELECT 
    current_trailing_term,
    -- 로그 변환 적용
    LN(NULLIF(current_trailing_term, 0)) as log_term,
    STDDEV(LN(NULLIF(current_trailing_term, 0))) OVER () as log_stddev,
    AVG(LN(NULLIF(current_trailing_term, 0))) OVER () as log_mean,
    STDDEV(current_trailing_term) OVER () as pop_stddev,
    AVG(current_trailing_term) OVER () as pop_mean
FROM survival_base
WHERE current_trailing_term IS NOT NULL
AND current_trailing_term > 0  -- 0 이하 제외
),

-- term_stats에서 로그 변환 추가
median_stats AS ( 
SELECT 
    APPROX_QUANTILES(current_trailing_term, 2)[OFFSET(1)] as median_term,
    -- 로그 변환된 중앙값 추가
    APPROX_QUANTILES(LN(NULLIF(current_trailing_term, 0)), 2)[OFFSET(1)] as log_median_term
FROM survival_base
WHERE current_trailing_term IS NOT NULL
AND current_trailing_term > 0
),

mad_stats AS (
SELECT
    -- 기존 MAD
    APPROX_QUANTILES(
    ABS(s.current_trailing_term - m.median_term), 
    2
    )[OFFSET(1)] as mad,
    -- 로그 변환된 MAD
    APPROX_QUANTILES(
    ABS(LN(NULLIF(s.current_trailing_term, 0)) - m.log_median_term),
    2
    )[OFFSET(1)] as log_mad
FROM survival_base s
CROSS JOIN median_stats m
WHERE s.current_trailing_term IS NOT NULL
AND s.current_trailing_term > 0
),

robust_bounds AS (
SELECT
    s.user_id,
    s.current_trailing_term,
    t.pop_mean,
    -- 로그 변환된 modified z-score 계산
    0.6745 * (LN(NULLIF(s.current_trailing_term, 0)) - m.log_median_term) / NULLIF(mad.log_mad, 0) as modified_zscore,
    s.days_since_last_purchase
FROM survival_base s
CROSS JOIN (SELECT DISTINCT pop_mean FROM term_stats) t
CROSS JOIN median_stats m
CROSS JOIN mad_stats mad
WHERE s.current_trailing_term IS NOT NULL
AND s.current_trailing_term > 0
),

churn_data AS (
SELECT 
    r.user_id,
    r.current_trailing_term,
    s.age_group,
    s.gender,
    CASE
    WHEN ABS(r.modified_zscore) > 3.5 THEN
        CASE WHEN r.days_since_last_purchase > r.pop_mean * 2 THEN 1 ELSE 0 END
    ELSE
        CASE WHEN r.days_since_last_purchase > r.current_trailing_term * 2 THEN 1 ELSE 0 END
    END as churn_flag
FROM robust_bounds r
LEFT JOIN survival_base s ON r.user_id = s.user_id
),

churn_group_count AS (
SELECT 
    age_group,
    gender,
    COUNT(*) AS group_user_count
FROM churn_data
GROUP BY age_group, gender
),

average_churn_rate AS (
SELECT 
    c.age_group,
    c.gender,
    SUM(c.churn_flag) / COUNT(*) AS avg_churn_rate,  -- 수정: 단순히 이탈한 사용자 비율 계산
    AVG(c.current_trailing_term) as avg_group_term
FROM churn_data c
GROUP BY c.age_group, c.gender
)

-- time_point 보간을 위한 기준 데이터 생성
,filled_time_points AS (
    SELECT DISTINCT 
        time_point,
        gender
    FROM (
        SELECT time_point
        FROM UNNEST(GENERATE_ARRAY(
            0, 
            (SELECT MAX(FLOOR(days_since_last_purchase / 7) * 7) FROM survival_base),
            7
        )) as time_point
    ) t
    CROSS JOIN (SELECT DISTINCT gender FROM survival_base)
),

-- 생존분석
survival_base_aggregated AS (
SELECT 
    FLOOR(s.days_since_last_purchase / 7) * 7 AS time_point, --  7일단위 코호트 이탈확률 도출
    s.gender,
    COUNT(*) AS n_risk,
    SUM(c.churn_flag) AS n_events
FROM survival_base s
LEFT JOIN churn_data c ON s.user_id = c.user_id
GROUP BY 
    FLOOR(s.days_since_last_purchase / 7) * 7,
    s.gender
)

-- 누락된 time_point 보간 처리
,interpolated_survival AS (
select *
from
    (SELECT 
        f.time_point,
        f.gender,
        COALESCE(s.n_risk,
            (LAG(s.n_risk) OVER (PARTITION BY f.gender ORDER BY f.time_point) +
            LEAD(s.n_risk) OVER (PARTITION BY f.gender ORDER BY f.time_point)) / 2
        ) as n_risk,
        COALESCE(s.n_events,
            (LAG(s.n_events) OVER (PARTITION BY f.gender ORDER BY f.time_point) +
            LEAD(s.n_events) OVER (PARTITION BY f.gender ORDER BY f.time_point)) / 2
        ) as n_events
    FROM filled_time_points f
    LEFT JOIN survival_base_aggregated s 
        ON f.time_point = s.time_point 
        AND f.gender = s.gender)
    where gender is not Null
)

-- 보간된 데이터로 KM 추정
,km_estimate AS (
SELECT 
    s.time_point,
    s.gender,
    s.n_risk,
    s.n_events,
    -- 기본 생존확률: 해당 시점에서의 생존율
    ROUND(
    CASE 
        WHEN s.n_risk > 0 THEN (1 - SAFE_DIVIDE(s.n_events, s.n_risk))
        ELSE 1 
    END, 
    4) as base_survival_prob,
    
    -- 가중 생존확률: 기본 생존율에 코호트별 평균 이탈률 반영
    ROUND(
    CASE 
        WHEN s.n_risk > 0 THEN 
        (1 - SAFE_DIVIDE(s.n_events, s.n_risk)) * 
        (1 - COALESCE(a.avg_churn_rate, 0))
        ELSE 1 
    END,
    4) as weighted_survival_prob,
    
    -- 누적 생존확률: 각 시점까지의 생존확률을 누적 곱
    ROUND(
    EXP(
        SUM(LN(
        GREATEST(
            CASE 
            WHEN s.n_risk > 0 THEN (1 - SAFE_DIVIDE(s.n_events, s.n_risk))
            ELSE 1
            END,
            0.0001
        )
        )) OVER (
        PARTITION BY s.gender 
        ORDER BY s.time_point
        )
    ), 
    4) as cumulative_survival_prob
FROM interpolated_survival s
LEFT JOIN (
    SELECT 
    gender,
    AVG(avg_churn_rate) as avg_churn_rate
    FROM average_churn_rate
    GROUP BY gender
) a ON s.gender = a.gender
)

,min_survival_prob AS (
SELECT 
    gender,
    MIN(cumulative_survival_prob) as min_survival_prob
FROM km_estimate
GROUP BY gender
)

,individual_survival AS (
SELECT 
    s.user_id,
    s.days_since_last_purchase,
    s.current_trailing_term,
    c.churn_flag,
    r.modified_zscore,
    s.age_group,
    s.gender,
    acr.avg_churn_rate,
    acr.avg_group_term,
    -- 생존확률 계산 수정
    CASE 
    WHEN s.gender IS NULL THEN
        -- gender가 NULL인 경우: 코호트 가중평균 미적용
        CASE
        WHEN ABS(r.modified_zscore) > 3.5 THEN 
            EXP(-r.days_since_last_purchase / r.pop_mean)
        ELSE
            EXP(-r.days_since_last_purchase / NULLIF(s.current_trailing_term, 0))
        END
    ELSE
        -- gender가 있는 경우: 코호트 가중평균 적용
        CASE
        WHEN ABS(r.modified_zscore) > 3.5 THEN 
            0.7 * EXP(-r.days_since_last_purchase / r.pop_mean) +
            0.3 * COALESCE(k.cumulative_survival_prob, m.min_survival_prob)
        ELSE
            0.7 * EXP(-r.days_since_last_purchase / NULLIF(s.current_trailing_term, 0)) +
            0.3 * COALESCE(k.cumulative_survival_prob, m.min_survival_prob)
        END
    END as survival_prob
FROM survival_base s
LEFT JOIN churn_data c ON s.user_id = c.user_id
LEFT JOIN robust_bounds r ON s.user_id = r.user_id
LEFT JOIN average_churn_rate acr 
    ON s.age_group = acr.age_group AND s.gender = acr.gender
LEFT JOIN km_estimate k 
    ON k.gender = s.gender 
    AND k.time_point = FLOOR(s.days_since_last_purchase / 7) * 7
LEFT JOIN min_survival_prob m
    ON m.gender = s.gender
),

final_analysis AS (
SELECT 
    s.user_id,
    s.days_since_last_purchase,
    s.age_group,
    s.gender,
    s.current_trailing_term,
    s.prev_trailing_term,
    round(s.cycle_stddev) as cycle_stddev,
    i.modified_zscore,
    i.avg_churn_rate as demographic_churn_rate,
    i.churn_flag,
    round(i.survival_prob,2) as survival_prob,
    CASE 
    WHEN ABS(i.modified_zscore) > 3.5 THEN 
        GREATEST(ROUND(-r.pop_mean * LN(0.5) - s.days_since_last_purchase),0)
    ELSE
        GREATEST(ROUND(-s.current_trailing_term * LN(0.5) - s.days_since_last_purchase),0)
    END AS predicted_survival_time,
    CASE 
    WHEN s.current_trailing_term <= 7 THEN '초단기'
    WHEN s.current_trailing_term <= 28 THEN '단기'
    WHEN s.current_trailing_term <= 60 THEN '중기'
    ELSE '장기'
    END as cycle_length,
    CASE 
    WHEN i.survival_prob <= 0.2 THEN 'High-Risk'
    WHEN i.survival_prob <= 0.5 THEN 'Medium-Risk'
    WHEN i.survival_prob <= 0.8 THEN 'Low-Risk'
    ELSE 'Safe'
    END AS risk_level
FROM survival_base s
LEFT JOIN individual_survival i ON s.user_id = i.user_id
LEFT JOIN robust_bounds r ON s.user_id = r.user_id
)
/* 위 로직 엉키지 않게 신규 유저 CTE 따로 생성. 기구매자의 대푯값 적용 */
,new_users AS (
SELECT 
    b.user_id,
    b.order_dt as latest_order_dt,
    date_diff({end_date_plus}, b.order_dt, day) as days_since_last_purchase,
    b.gender,
    CAST(FLOOR((EXTRACT(YEAR FROM date({end_date_plus})) - SAFE_CAST(b.birth_year AS INT64)) / 10) * 10 AS STRING) as age_group
FROM base_data b
WHERE b.recency_rank = 1
AND NOT EXISTS (
    SELECT 1 FROM purchase_intervals p 
    WHERE p.user_id = b.user_id 
    AND p.purchase_rank > 1
)
)

,new_user_stats AS (
select *,
    case 
    when survival_prob <= 0.2 then 'High-Risk'
    when survival_prob <= 0.5 then 'Medium-Risk'
    when survival_prob <= 0.8 then 'Low-Risk'
    else 'Safe'
    end as risk_level
from
    (SELECT 
    n.user_id,
    n.days_since_last_purchase,
    n.age_group,
    n.gender,
    NULL as current_trailing_term,
    NULL as prev_trailing_term,
    NULL as cycle_stddev,
    NULL as modified_zscore,
    NULL as demographic_churn_rate,
    NULL as churn_flag,
    -- 생존 확률 계산 수정: NULL일 경우 성별별 최소값 사용
    case 
        when n.gender is not Null then
        0.7 * EXP(-n.days_since_last_purchase / t.pop_mean) +
        0.3 * COALESCE(k.cumulative_survival_prob, m.min_survival_prob) 
        else
        EXP(-n.days_since_last_purchase / t.pop_mean)
        end as survival_prob,
    GREATEST(-t.pop_mean * LN(0.5) - n.days_since_last_purchase, 0) as predicted_survival_time,

    '신규' as cycle_length
    FROM new_users n
    CROSS JOIN (SELECT DISTINCT pop_mean FROM term_stats) t
    LEFT JOIN km_estimate k 
    ON k.gender = n.gender 
    AND k.time_point = FLOOR(n.days_since_last_purchase / 7) * 7
    LEFT JOIN min_survival_prob m
    ON m.gender = n.gender)
)

SELECT *, {end_date_plus} as cur FROM new_user_stats
union all
SELECT *, {end_date_plus} as cur FROM final_analysis
)
