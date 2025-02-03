create or replace table ballosodeuk.ynam.rfm_table_shopby_rfm_target as (
    WITH r_table AS (
    SELECT *
    FROM ballosodeuk.ynam.rfm_table_merged_shopby
    ),

    rfm_scores AS (
    SELECT 
        tgt,
        member_no,
        user_id,
        first_order_dt,
        refund_rate,
        gender,
        CONCAT(
        FORMAT_DATE('%y', join_dt),
        '-',
        FORMAT_DATE('%m', join_dt)
        ) as join_group,
        CAST(FLOOR((EXTRACT(YEAR FROM date({event_date_plus})) - SAFE_CAST(birth_year AS INT64)) / 10) * 10 AS STRING) as age_group,
        platform,
        total_accumulate_cash,
        terms_agree_yn,
        total_accumulate_shoji,
        pre_cash,
        current_cash,
        current_shoji,
        earn,
        spend,
        exchange,
        exchange_cash_rate,
        burnt,
        r_shop,
        f_shop,
        m_shop,
        current_trailing_term_shop,
        prev_trailing_term_shop,
        cycle_stddev_shop,
        risk_level_shop,
        round(suvival_prob_shop,2) as suvival_prob_shop,
        cycle_length_shop,
        round(predicted_survival_time_shop) as predicted_survival_time_shop,

        -- Recency 점수: 낮을수록 최근 → 높은 점수 (1=낮은, 5=높은)
        NTILE(10) OVER (ORDER BY r_shop DESC) AS recency_ntile,

        -- Frequency 점수: 높을수록 → 높은 점수 (1=낮은, 5=높은)
        -- 1/9 로직 변경 >> 1회구매자가 너무 많아 로그 변환으로 변별력 부여
        CASE 
            WHEN f_shop = 1 THEN 1
            ELSE NTILE(9) OVER (
                PARTITION BY CASE WHEN f_shop > 1 THEN 1 END 
                ORDER BY LN(f_shop)) + 1
            END AS f_ntile,

        -- Monetary 점수: 높을수록 → 높은 점수 (1=낮은, 5=높은)
        NTILE(10) OVER (ORDER BY m_shop ASC) AS m_ntile,

        -- Current Trailing Term: 작을수록 → 높은 점수 (1=긴, 5=짧은)
        CASE
            WHEN current_trailing_term_shop is Null then 1
            ELSE
                NTILE(9) OVER (
                    PARTITION BY CASE WHEN current_trailing_term_shop is not Null then 1 END
                    ORDER BY current_trailing_term_shop DESC) + 1
            END AS term_ntile,

        -- Term Difference: 작거나 음수일수록 → 높은 점수 
        CASE
            WHEN prev_trailing_term_shop is Null then 1
            ELSE
                NTILE(9) OVER (
                    PARTITION BY CASE WHEN 
                        (current_trailing_term_shop - prev_trailing_term_shop) is not Null then 1 END
                    ORDER BY (current_trailing_term_shop - prev_trailing_term_shop) DESC) + 1
            END AS term_diff_ntile,

        -- Volatility: 작을수록 → 높은 점수 
        CASE
            WHEN prev_trailing_term_shop is Null then 1
            ELSE
                NTILE(9) OVER (
                    PARTITION BY CASE WHEN current_trailing_term_shop is not Null then 1 END
                    ORDER BY cycle_stddev_shop DESC) + 1
            END AS volatility_ntile

        ,ranking_1_1_sp as ranking_1_1
        ,ranking_1_2_sp as ranking_1_2
        ,ranking_1_3_sp as ranking_1_3
        ,power_1_1_sp as power_1_1
        ,power_1_2_sp as power_1_2
        ,power_1_3_sp as power_1_3
        ,ranking_2_1_sp as ranking_2_1
        ,ranking_2_2_sp as ranking_2_2
        ,ranking_2_3_sp as ranking_2_3
        ,ranking_2_3_sp as ranking_2_4
        ,ranking_2_3_sp as ranking_2_5
        ,ranking_2_3_sp as ranking_2_6
        ,power_2_1_sp as power_2_1
        ,power_2_2_sp as power_2_2
        ,power_2_3_sp as power_2_3
        ,power_2_3_sp as power_2_4
        ,power_2_3_sp as power_2_5
        ,power_2_3_sp as power_2_6
    FROM r_table
    WHERE f_shop > 0
    )

    ,final_score AS (

        SELECT
            tgt,
            user_id,
            member_no,
            --  유저 세그
            gender,
            join_group,
            age_group,
            platform,
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
            first_order_dt,
            terms_agree_yn,
            --  매출 세그
            r_shop,
            f_shop,
            m_shop,

            round(m_shop / f_shop,2) as ar_shop,
            current_trailing_term_shop,
            prev_trailing_term_shop,
            current_trailing_term_shop - prev_trailing_term_shop as term_diff_shop,
            cycle_stddev_shop,
            refund_rate,

            -- 점수 매김: NTILE이 클수록 높은 점수 (1~5)
            recency_ntile AS r_score,
            f_ntile AS f_score,
            m_ntile AS m_score,
            term_ntile AS term_score,
            term_diff_ntile AS term_diff_score,
            volatility_ntile AS volatility_score, 

            -- 총점 계산 (가중치 적용 예시)
            ROUND(
            (recency_ntile * 0.22) + 
            (f_ntile * 0.427) + 
            (m_ntile * 0.35) + 
            (COALESCE(term_ntile, 1) * 0.0) + 
            (COALESCE(term_diff_ntile, 1) * 0.00) + 
            (COALESCE(volatility_ntile, 1) * 0.00), 2
            ) AS total_score,

            cycle_length_shop,
            risk_level_shop,
            suvival_prob_shop,
            predicted_survival_time_shop
        ,ranking_1_1
        ,ranking_1_2
        ,ranking_1_3
        ,power_1_1
        ,power_1_2
        ,power_1_3
        ,ranking_2_1
        ,ranking_2_2
        ,ranking_2_3
        ,ranking_2_4
        ,ranking_2_5
        ,ranking_2_6
        ,power_2_1
        ,power_2_2
        ,power_2_3
        ,power_2_4
        ,power_2_5
        ,power_2_6
        ,{event_date_plus} as snapshot_dt
        FROM rfm_scores
    ) 

    select *,
        -- 퍼센타일 적용
        percentile_cont(total_score, 0.4) over () as cut1,
        percentile_cont(total_score, 0.7) over () as cut2,
        percentile_cont(total_score, 0.9) over () as cut3,
        -- 최신 등급 적용
        case 
            when total_score > percentile_cont(total_score, 0.9) over () then 'VIP'
            when total_score > percentile_cont(total_score, 0.7) over () then 'GOLD'
            when total_score > percentile_cont(total_score, 0.4) over () then 'SILVER'
        else 'IRON'
        end as grade
    from final_score
)
