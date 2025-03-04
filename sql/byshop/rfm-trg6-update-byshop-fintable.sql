CREATE table if not Exists `ballosodeuk.ynam.rfm_byshop_history_array_table_tst` (
    user_id STRING,
    -- 변경이 적은 기본 정보
    gender STRING,
    age_group STRING,
    join_group STRING,
    platform STRING,
    terms_agree_yn STRING,

    -- 스코어/상태 이력
    score_history ARRAY<STRUCT<
    snapshot_dt DATE,  
    tgt STRING,
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

    survival_prob FLOAT64,
    predicted_survival_time FLOAT64,
    total_accumulate_cash INT64,
    total_accumulate_shoji INT64,
    current_cash INT64,
    current_shoji INT64
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

    MERGE `ballosodeuk.ynam.rfm_byshop_history_array_table_tst` T
    USING (
    SELECT 
        user_id,
        gender,
        age_group,
        join_group,
        platform,
        terms_agree_yn,
        tgt,
        CAST(snapshot_dt as DATE) as snapshot_dt,
        CAST(total_score AS FLOAT64) as total_score,
        CAST(cut1 AS FLOAT64) as cut1,
        CAST(cut2 AS FLOAT64) as cut2,
        CAST(cut3 AS FLOAT64) as cut3,
        grade,
        risk_level_byshop as risk_level,
        cycle_length_byshop as cycle_length,
        CAST(r_score AS INT64) as r_score,
        CAST(f_score AS INT64) as f_score,
        CAST(m_score AS INT64) as m_score,
        CAST(term_score AS INT64) as term_score,
        CAST(term_diff_score AS INT64) as term_diff_score,
        CAST(volatility_score AS INT64) as volatility_score,
        CAST(r_byshop AS INT64) as r_data,
        CAST(f_byshop AS INT64) as f_data,
        CAST(m_byshop AS INT64) as m_data,
        CAST(ar_byshop AS FLOAT64) as ar_data,
        CAST(current_trailing_term_byshop AS FLOAT64) as current_trailing_term,
        CAST(prev_trailing_term_byshop AS FLOAT64) as prev_trailing_term,
        CAST(term_diff_score AS FLOAT64) as term_diff,
        CAST(cycle_stddev_byshop AS FLOAT64) as cycle_stddev,
        CAST(suvival_prob_byshop AS FLOAT64) as survival_prob,
        CAST(predicted_survival_time_byshop AS FLOAT64) as predicted_survival_time,
        CAST(total_accumulate_cash AS INT64) as total_accumulate_cash,
        CAST(total_accumulate_shoji AS INT64) as total_accumulate_shoji,
        CAST(current_cash AS INT64) as current_cash,
        CAST(current_shoji AS INT64) as current_shoji,
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
        power_2_6
    FROM `ballosodeuk.ynam.rfm_table_byshop_rfm_target`
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
        
        score_history = ARRAY_CONCAT(
        IFNULL(T.score_history, []), 
        [STRUCT(
            S.snapshot_dt,
            S.tgt,
            S.total_score,
            S.cut1,
            S.cut2,
            S.cut3,
            S.grade,
            S.risk_level,
            S.cycle_length,
            S.r_score,
            S.f_score,
            S.m_score,
            S.term_score,
            S.term_diff_score,
            S.volatility_score,
            S.r_data,
            S.f_data,
            S.m_data,
            S.ar_data,
            S.current_trailing_term,
            S.prev_trailing_term,
            S.term_diff,
            S.cycle_stddev,
            S.survival_prob,
            S.predicted_survival_time,
            S.total_accumulate_cash,
            S.total_accumulate_shoji,
            S.current_cash,
            S.current_shoji
        )]),
        
        category_history = ARRAY_CONCAT(
        IFNULL(T.category_history, []), 
        [STRUCT(
            S.snapshot_dt,
            S.ranking_1_1,
            S.ranking_1_2,
            S.ranking_1_3,
            S.power_1_1,
            S.power_1_2,
            S.power_1_3,
            S.ranking_2_1,
            S.ranking_2_2,
            S.ranking_2_3,
            S.ranking_2_4,
            S.ranking_2_5,
            S.ranking_2_6,
            S.power_2_1,
            S.power_2_2,
            S.power_2_3,
            S.power_2_4,
            S.power_2_5,
            S.power_2_6
        )])
    WHEN NOT MATCHED THEN
    INSERT (
        user_id, 
        gender,
        age_group,
        join_group,
        platform,
        terms_agree_yn,
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
        [STRUCT(
            S.snapshot_dt,
            S.tgt,
            S.total_score,
            S.cut1,
            S.cut2,
            S.cut3,
            S.grade,
            S.risk_level,
            S.cycle_length,
            S.r_score,
            S.f_score,
            S.m_score,
            S.term_score,
            S.term_diff_score,
            S.volatility_score,
            S.r_data,
            S.f_data,
            S.m_data,
            S.ar_data,
            S.current_trailing_term,
            S.prev_trailing_term,
            S.term_diff,
            S.cycle_stddev,
            S.survival_prob,
            S.predicted_survival_time,
            S.total_accumulate_cash,
            S.total_accumulate_shoji,
            S.current_cash,
            S.current_shoji
        )],
        [STRUCT(
            S.snapshot_dt,
            S.ranking_1_1,
            S.ranking_1_2,
            S.ranking_1_3,
            S.power_1_1,
            S.power_1_2,
            S.power_1_3,
            S.ranking_2_1,
            S.ranking_2_2,
            S.ranking_2_3,
            S.ranking_2_4,
            S.ranking_2_5,
            S.ranking_2_6,
            S.power_2_1,
            S.power_2_2,
            S.power_2_3,
            S.power_2_4,
            S.power_2_5,
            S.power_2_6
        )]
  )
