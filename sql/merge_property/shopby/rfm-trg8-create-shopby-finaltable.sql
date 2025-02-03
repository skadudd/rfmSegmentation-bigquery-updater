  create or replace table `ballosodeuk.ynam.rfm_table_shopby_rfm_finaltable` as (
    SELECT 
        a.user_id,
        a.gender,
        a.age_group,
        a.join_group,
        a.platform,
        a.terms_agree_yn,
        a.first_order_dt,
        ARRAY_AGG(
        STRUCT(
            b.snapshot_dt,
            b.total_score,
            b.cut1,
            b.cut2,
            b.cut3,
            b.grade,
            b.risk_level,
            b.cycle_length,
            b.r_score,
            b.f_score,
            b.m_score,
            b.term_score,
            b.term_diff_score,
            b.volatility_score,
            b.r_data,
            b.f_data,
            b.m_data,
            b.ar_data,
            b.current_trailing_term,
            b.prev_trailing_term,
            b.term_diff,
            b.cycle_stddev,
            b.refund_rate,
            b.survival_prob,
            b.predicted_survival_time,
            b.total_accumulate_cash,
            b.total_accumulate_shoji,
            b.current_cash,
            b.current_shoji
        ) ORDER BY b.snapshot_dt
        ) as score_history,
        ARRAY_AGG(
        STRUCT(
        b.snapshot_dt,
        c.register_dt,
        c.pre_cash,
        c.current_cash,
        c.pre_shoji,
        c.current_shoji,
        c.earn,
        c.spend,
        c.exchange,
        c.exchange_cash_rate,
        c.burnt
        )  ORDER BY b.snapshot_dt
        ) as property_history,
        a.category_history
    FROM ballosodeuk.ynam.rfm_shopby_history_array_table a, 
    UNNEST(score_history) b
    LEFT JOIN ballosodeuk.ynam.rfm_table_shopby_rfm_properties c 
        ON a.user_id = c.user_id 
        AND b.snapshot_dt = c.register_dt
    GROUP BY 
        a.user_id,
        a.gender,
        a.age_group,
        a.join_group,
        a.platform,
        a.terms_agree_yn,
        a.first_order_dt,
        a.category_history
  )