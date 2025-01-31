create or replace table ballosodeuk.ynam.rfm_table_byshop_survive_prop as (
with base_data AS (
    SELECT 
        user_id,
        event_dt as order_dt,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_dt DESC) as recency_rank,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_dt ASC) as purchase_rank
    FROM ballosodeuk.ynam.rfm_table_byshop_before_survive_prop
    GROUP BY user_id, event_dt
),

recent_purchase as (
    SELECT 
        user_id,
        order_dt as latest_order_dt,
        date_diff(date({end_date_plus}), order_dt, day) as days_since_last_purchase
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
        ROUND((s.cycle_stddev / NULLIF(c.current_trailing_term, 0)) * 100, 1) as cycle_variation_rate
    FROM current_trailing c
    LEFT JOIN prev_trailing p ON c.user_id = p.user_id
    LEFT JOIN user_stats s ON c.user_id = s.user_id
    LEFT JOIN recent_purchase r ON c.user_id = r.user_id
),

term_stats AS (
    SELECT 
        current_trailing_term,
        LN(NULLIF(current_trailing_term, 0)) as log_term,
        STDDEV(LN(NULLIF(current_trailing_term, 0))) OVER () as log_stddev,
        AVG(LN(NULLIF(current_trailing_term, 0))) OVER () as log_mean,
        STDDEV(current_trailing_term) OVER () as pop_stddev,
        AVG(current_trailing_term) OVER () as pop_mean
    FROM survival_base
    WHERE current_trailing_term IS NOT NULL
    AND current_trailing_term > 0
),

median_stats AS ( 
    SELECT 
        APPROX_QUANTILES(current_trailing_term, 2)[OFFSET(1)] as median_term,
        APPROX_QUANTILES(LN(NULLIF(current_trailing_term, 0)), 2)[OFFSET(1)] as log_median_term
    FROM survival_base
    WHERE current_trailing_term IS NOT NULL
    AND current_trailing_term > 0
),

mad_stats AS (
    SELECT
        APPROX_QUANTILES(
            ABS(s.current_trailing_term - m.median_term), 
            2
        )[OFFSET(1)] as mad,
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
        CASE
            WHEN ABS(r.modified_zscore) > 3.5 THEN
                CASE WHEN r.days_since_last_purchase > r.pop_mean * 2 THEN 1 ELSE 0 END
            ELSE
                CASE WHEN r.days_since_last_purchase > r.current_trailing_term * 2 THEN 1 ELSE 0 END
        END as churn_flag
    FROM robust_bounds r
),

average_churn_rate AS (
    SELECT 
        SUM(churn_flag) / COUNT(*) AS avg_churn_rate,  
        AVG(current_trailing_term) as avg_group_term
    FROM churn_data
),

filled_time_points AS (
    SELECT DISTINCT time_point
    FROM UNNEST(GENERATE_ARRAY(
        0, 
        (SELECT MAX(FLOOR(days_since_last_purchase / 7) * 7) FROM survival_base),
        7
    )) as time_point
),

survival_base_aggregated AS (
    SELECT 
        FLOOR(s.days_since_last_purchase / 7) * 7 AS time_point,
        COUNT(*) AS n_risk,
        SUM(c.churn_flag) AS n_events
    FROM survival_base s
    LEFT JOIN churn_data c ON s.user_id = c.user_id
    GROUP BY FLOOR(s.days_since_last_purchase / 7) * 7
),

km_estimate AS (
    SELECT 
        time_point,
        n_risk,
        n_events,
        ROUND(
            CASE 
                WHEN n_risk > 0 THEN (1 - SAFE_DIVIDE(n_events, n_risk))
                ELSE 1 
            END, 
        4) as base_survival_prob,
        
        ROUND(
            CASE 
                WHEN n_risk > 0 THEN 
                    (1 - SAFE_DIVIDE(n_events, n_risk)) * 
                    (1 - COALESCE((SELECT avg_churn_rate FROM average_churn_rate), 0))
                ELSE 1 
            END,
        4) as weighted_survival_prob,
        
        ROUND(
            EXP(
                SUM(LN(
                    GREATEST(
                        CASE 
                            WHEN n_risk > 0 THEN (1 - SAFE_DIVIDE(n_events, n_risk))
                            ELSE 1
                        END,
                        0.0001
                    )
                )) OVER (ORDER BY time_point)
            ), 
        4) as cumulative_survival_prob
    FROM survival_base_aggregated
),

min_survival_prob AS (
    SELECT MIN(cumulative_survival_prob) as min_survival_prob
    FROM km_estimate
),

individual_survival AS (
    SELECT 
        s.user_id,
        s.days_since_last_purchase,
        s.current_trailing_term,
        s.prev_trailing_term,
        c.churn_flag,
        r.modified_zscore,
        acr.avg_churn_rate,
        acr.avg_group_term,
        CASE 
            WHEN ABS(r.modified_zscore) > 3.5 THEN 
                0.7 * EXP(-r.days_since_last_purchase / r.pop_mean) +
                0.3 * COALESCE(k.cumulative_survival_prob, m.min_survival_prob)
            ELSE
                0.7 * EXP(-r.days_since_last_purchase / NULLIF(s.current_trailing_term, 0)) +
                0.3 * COALESCE(k.cumulative_survival_prob, m.min_survival_prob)
        END as survival_prob
    FROM survival_base s
    LEFT JOIN churn_data c ON s.user_id = c.user_id
    LEFT JOIN robust_bounds r ON s.user_id = r.user_id
    CROSS JOIN average_churn_rate acr 
    LEFT JOIN km_estimate k 
        ON k.time_point = FLOOR(s.days_since_last_purchase / 7) * 7
    CROSS JOIN min_survival_prob m
),

final_analysis AS (
    SELECT 
        s.user_id,
        s.days_since_last_purchase,
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
),

new_users AS (
    SELECT 
        b.user_id,
        b.order_dt as latest_order_dt,
        date_diff(date({end_date_plus}), b.order_dt, day) as days_since_last_purchase
    FROM base_data b
    WHERE b.recency_rank = 1
    AND NOT EXISTS (
        SELECT 1 FROM purchase_intervals p 
        WHERE p.user_id = b.user_id 
        AND p.purchase_rank > 1
    )
),

new_user_stats AS (
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
            NULL as current_trailing_term,
            NULL as prev_trailing_term,
            NULL as cycle_stddev,
            NULL as modified_zscore,
            NULL as demographic_churn_rate,
            NULL as churn_flag,
            0.7 * EXP(-n.days_since_last_purchase / t.pop_mean) +
            0.3 * COALESCE(k.cumulative_survival_prob, m.min_survival_prob) as survival_prob,
            GREATEST(-t.pop_mean * LN(0.5) - n.days_since_last_purchase, 0) as predicted_survival_time,
            '신규' as cycle_length
        FROM new_users n
        CROSS JOIN (SELECT DISTINCT pop_mean FROM term_stats) t
        LEFT JOIN km_estimate k 
            ON k.time_point = FLOOR(n.days_since_last_purchase / 7) * 7
        CROSS JOIN min_survival_prob m)
)

SELECT *, date({end_date_plus}) as cur FROM new_user_stats
UNION ALL
SELECT *, date({end_date_plus}) as cur FROM final_analysis
)
