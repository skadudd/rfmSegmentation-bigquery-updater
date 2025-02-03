CREATE table if not Exists `ballosodeuk.ynam.rfm_shopby_history_array_table` (
user_id STRING,
-- 변경이 적은 기본 정보
gender STRING,
age_group STRING,
join_group STRING,
platform STRING,
terms_agree_yn STRING,
first_order_dt DATE,
-- 스코어/상태 이력
score_history ARRAY<STRUCT<
    snapshot_dt DATE,
    total_score FLOAT64,
    cut1 FLOAT64,
    cut2 FLOAT64,
    cut3 FLOAT64,
    grade STRING,
    risk_level STRING,
    
    cycle_length STRING,
    r_score INT64,
    f_score INT64,
    m_score INT64,
    term_score INT64,
    term_diff_score INT64,
    volatility_score INT64,
    r_data INT64,
    f_data INT64,
    m_data INT64,
    ar_data FLOAT64,
    current_trailing_term FLOAT64,
    prev_trailing_term FLOAT64,
    term_diff FLOAT64,
    cycle_stddev FLOAT64,
    refund_rate FLOAT64,
    survival_prob FLOAT64,
    predicted_survival_time FLOAT64,
    total_accumulate_cash INT64,
    total_accumulate_shoji INT64,
    pre_cash INT64,
    current_cash INT64,
    current_shoji INT64
    ,earn INT64
    -- ,spend INT64
    -- ,exchange INT64
    -- ,exchange_cash_rate FLOAT64
    -- ,burnt INT64
>>,

-- 카테고리 관심도 이력
category_history ARRAY<STRUCT<
    snapshot_dt DATE,
    ranking_1_1 STRING,
    ranking_1_2 STRING,
    ranking_1_3 STRING,
    power_1_1 FLOAT64,
    power_1_2 FLOAT64,
    power_1_3 FLOAT64,
    ranking_2_1 STRING,
    ranking_2_2 STRING,
    ranking_2_3 STRING,
    ranking_2_4 STRING,
    ranking_2_5 STRING,
    ranking_2_6 STRING,
    power_2_1 FLOAT64,
    power_2_2 FLOAT64,
    power_2_3 FLOAT64,
    power_2_4 FLOAT64,
    power_2_5 FLOAT64,
    power_2_6 FLOAT64
>>
);

MERGE `ballosodeuk.ynam.rfm_shopby_history_array_table` T
USING (
    SELECT 
        user_id,
        gender,
        age_group,
        join_group,
        platform,
        terms_agree_yn,
        CAST(total_score AS FLOAT64) as total_score, 
        first_order_dt,
        refund_rate,
        r_score,
        f_score,
        m_score,
        cut1,
        cut2,
        cut3,
        grade,
        risk_level_shop as risk_level,
        current_trailing_term_shop as current_trailing_term,
        prev_trailing_term_shop as prev_trailing_term,
        term_diff_shop as term_diff,
        cycle_stddev_shop as cycle_stddev,
        ar_shop as ar_data,

        term_score,
        term_diff_score,
        volatility_score,
        cycle_length_shop AS cycle_length,
        r_shop AS r_data,
        f_shop AS f_data,
        m_shop AS m_data,
        suvival_prob_shop AS survival_prob,
        predicted_survival_time_shop AS predicted_survival_time,
        total_accumulate_cash,
        total_accumulate_shoji,
        pre_cash,
        current_cash,
        current_shoji,
        earn,
        spend,
        exchange,
        exchange_cash_rate,
        burnt,
        ranking_1_1,
        ranking_1_2,
        ranking_1_3,
        power_1_1,
        power_1_2,
        power_1_3,
        ranking_2_1,
        ranking_2_2,
        ranking_2_3,
        ranking_2_4,
        ranking_2_5,
        ranking_2_6,
        power_2_1,
        power_2_2, 
        power_2_3,
        power_2_4,
        power_2_5,
        power_2_6,
        CAST(snapshot_dt as DATE) as snapshot_dt
    FROM `ballosodeuk.ynam.rfm_table_shopby_rfm_target`
    where user_id is not Null
) S
ON T.user_id = S.user_id
WHEN MATCHED THEN
    UPDATE
    SET
        gender = S.gender,
        age_group = S.age_group,
        join_group = S.join_group,
        platform = S.platform,
        terms_agree_yn = S.terms_agree_yn,
        first_order_dt = S.first_order_dt,

        score_history = ARRAY_CONCAT(
        T.score_history, 
        [STRUCT(
            S.snapshot_dt as snapshot_dt,
            S.total_score as total_score,
            S.cut1,
            S.cut2,
            S.cut3,
            S.grade,
            S.risk_level as risk_level,

            S.cycle_length as cycle_length,
            S.r_score as r_score,
            S.f_score as f_score,
            S.m_score as m_score,
            S.term_score as term_score,
            S.term_diff_score as term_diff_score,
            S.volatility_score as volatility_score,
            S.r_data as r_data,
            S.f_data as f_data,
            S.m_data as m_data,
            S.ar_data as ar_data,
            S.current_trailing_term as current_trailing_term,
            S.prev_trailing_term as prev_trailing_term,
            S.term_diff as term_diff,
            S.cycle_stddev as cycle_stddev,
            S.refund_rate as refund_rate,
            S.survival_prob as survival_prob,
            S.predicted_survival_time as predicted_survival_time,
            S.total_accumulate_cash as total_accumulate_cash,
            S.total_accumulate_shoji as total_accumulate_shoji,
            S.pre_cash as pre_cash,
            S.current_cash as current_cash,
            S.current_shoji as current_shoji
            ,S.earn as earn
            -- ,S.spend as spend
            -- ,S.exchange as exchange
            -- ,S.exchange_cash_rate as exchange_cash_rate
            -- ,S.burnt as burnt
        )]
        ),
        
        category_history = ARRAY_CONCAT(
        T.category_history, 
        [STRUCT(
            S.snapshot_dt as snapshot_dt,
            S.ranking_1_1 as ranking_1_1,
            S.ranking_1_2 as ranking_1_2,
            S.ranking_1_3 as ranking_1_3,
            S.power_1_1 as power_1_1,
            S.power_1_2 as power_1_2,
            S.power_1_3 as power_1_3,
            S.ranking_2_1 as ranking_2_1,
            S.ranking_2_2 as ranking_2_2,
            S.ranking_2_3 as ranking_2_3,
            S.ranking_2_4 as ranking_2_4,
            S.ranking_2_5 as ranking_2_5,
            S.ranking_2_6 as ranking_2_6,
            S.power_2_1 as power_2_1,
            S.power_2_2 as power_2_2,
            S.power_2_3 as power_2_3,
            S.power_2_4 as power_2_4,
            S.power_2_5 as power_2_5,
            S.power_2_6 as power_2_6
        )]
        )
WHEN NOT MATCHED THEN
    INSERT (
        user_id, 
        gender,
        age_group,
        join_group,
        platform,
        terms_agree_yn,
        first_order_dt,
        score_history, 
        category_history
    )
    VALUES (
        S.user_id,
        S.gender,
        S.age_group,
        S.join_group,
        S.platform,
        S.terms_agree_yn,
        first_order_dt,
        [STRUCT(
            S.snapshot_dt as snapshot_dt,
            S.total_score as total_score,
            S.cut1,
            S.cut2,
            S.cut3,
            S.grade,    
            S.risk_level as risk_level,

            S.cycle_length as cycle_length,
            S.r_score as r_score,
            S.f_score as f_score,
            S.m_score as m_score,
            S.term_score as term_score,
            S.term_diff_score as term_diff_score,
            S.volatility_score as volatility_score,
            S.r_data as r_data,
            S.f_data as f_data,
            S.m_data as m_data,
            S.ar_data as ar_data,
            S.current_trailing_term as current_trailing_term,
            S.prev_trailing_term as prev_trailing_term,
            S.term_diff as term_diff,
            S.cycle_stddev as cycle_stddev,
            S.refund_rate as refund_rate,
            S.survival_prob as survival_prob,
            S.predicted_survival_time as predicted_survival_time,
            S.total_accumulate_cash as total_accumulate_cash,
            S.total_accumulate_shoji as total_accumulate_shoji,
            S.pre_cash as pre_cash,
            S.current_cash as current_cash,
            S.current_shoji as current_shoji
            ,S.earn as earn
            -- ,S.spend as spend
            -- ,S.exchange as exchange
            -- ,S.exchange_cash_rate as exchange_cash_rate
            -- ,S.burnt as burnt
        )],
        [STRUCT(
            S.snapshot_dt as snapshot_dt,
            S.ranking_1_1 as ranking_1_1,
            S.ranking_1_2 as ranking_1_2,
            S.ranking_1_3 as ranking_1_3,
            S.power_1_1 as power_1_1,
            S.power_1_2 as power_1_2,
            S.power_1_3 as power_1_3,
            S.ranking_2_1 as ranking_2_1,
            S.ranking_2_2 as ranking_2_2,
            S.ranking_2_3 as ranking_2_3,
            S.ranking_2_4 as ranking_2_4,
            S.ranking_2_5 as ranking_2_5,
            S.ranking_2_6 as ranking_2_6,
            S.power_2_1 as power_2_1,
            S.power_2_2 as power_2_2,
            S.power_2_3 as power_2_3,
            S.power_2_4 as power_2_4,
            S.power_2_5 as power_2_5,
            S.power_2_6 as power_2_6
        )]
    )