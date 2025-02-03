create or replace table `ballosodeuk.ynam.rfm_table_shopby_rfm_properties` as (
  with raw as 
          (select
              register_dttm, register_dt, b.member_no, amt, accumulation_status, reason, reason_detail, mapping_type, mapping_value, related_accumulation_no, reg_dttm, mod_dttm,
          from ballosodeuk.dw.fact_shopby_reward a
          inner join
          (
              select order_dt, member_no
              from ballosodeuk.dw.fact_shopby_order
              WHERE order_dt between date("2024-10-01") and date({end_date})
          ) b on a.member_no = b.member_no
          WHERE order_dt between date("2024-10-01") and date({end_date})
          group by register_dttm, register_dt, member_no, amt, accumulation_status, reason, reason_detail, mapping_type, mapping_value, related_accumulation_no, reg_dttm, mod_dttm
          )

      ,daily_net_amount as 
          (select 
              register_dt, member_no, sum(earn) as earn, coalesce(sum(spend),0) as spend, sum(coalesce(earn,0) - coalesce(spend,0)) as num
          from
              (SELECT 
                  register_dt, member_no,
                      CASE WHEN accumulation_status IN ('취소로 인한 지급','지급') THEN amt END as earn,
                      CASE WHEN accumulation_status IN ('차감') THEN amt END as spend
              FROM raw
              )
          group by register_dt, member_no
          )
      
      ,exception_case as (
      select register_dt, user_id, sum(amt) as amt, "소멸" as reason
      from 
          (select register_dt, a.member_no, wk_id as user_id, amt, reason
          from ballosodeuk.dw.fact_shopby_reward a
          inner join ballosodeuk.dw.dim_shopby_member b on a.member_no = b.member_no
          where reason ="유효기간 만료")
      group by register_dt, user_id
      )

      ,exception_case_2 as (
      select register_dt, user_id, sum(amt) as amt, "교환권" as reason
      from
          (select register_dt, a.member_no, wk_id as user_id, amt
          from ballosodeuk.dw.fact_shopby_reward a
          inner join ballosodeuk.dw.dim_shopby_member b on a.member_no = b.member_no
          where 1=1
          and reason = "운영자 지급" 
          and reason_detail like "%교환%" or reason_detail in("쇼핑지원금 상품권 적립","쇼핑지원금 교환권 적립","쇼핑지원금 환전","쇼핑지원금 5,000원 교환 쿠폰","쇼핑지원금 전환"))
      group by register_dt, user_id
      )

      ,cash_case as (
      select user_id, current_cash
      from ballosodeuk.dw.dim_airbridge_member
      )

      ,cumulative_amount as (
          select register_dt, member_no, num as daily_net_amount,
              sum(num) over (
                  partition by member_no
                  order by register_dt
                  rows between unbounded preceding and current row
              ) as cummulative_amount
              ,earn, spend
          from daily_net_amount
      )

      ,merged_ as 
          (select 
              b.wk_id as user_id, a.member_no,gender,
              cast(floor(
                  DATE_DIFF(
                  DATE(FORMAT_DATE('%Y-01-01', CURRENT_DATE())), -- 현재 년도의 1월 1일
                  DATE(SAFE_CAST(birth_year AS INT64), 1, 1),    -- birth_year의 1월 1일
                  YEAR  -- 년 단위로 차이 계산
                  ) / 10) * 10 as int64) as age
              ,a.* except(member_no)
          from cumulative_amount a
          inner join ballosodeuk.dw.dim_shopby_member b on a.member_no = b.member_no
          order by user_id, register_dt
          )

      ,df as 
      (select 
          a.register_dt, a.user_id,gender,age, member_no, cummulative_amount as current_shoji
          ,cast(current_cash as int64) + COALESCE(sum(d.amt) over (partition by a.user_id) * 2, 0) as pre_cash
          ,c.amt as burnt, d.amt as exchange, d.amt * 2 as exchange_cash_rate, earn, spend
      from merged_ a
      left join exception_case c on c.user_id = a.user_id and a.register_dt = c.register_dt
      left join exception_case_2 d on d.user_id = a.user_id and a.register_dt = d.register_dt
      left join cash_case e on e.user_id = a.user_id
      )

      ,fin_df as 
      (select 
          register_dt, user_id, member_no ,gender, age
          ,pre_cash - 
              COALESCE(
                  SUM(exchange_cash_rate) OVER (
                      PARTITION BY member_no
                      ORDER BY register_dt
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                  , 0) + 
              COALESCE(exchange_cash_rate,0) as pre_cash
          ,pre_cash - 
              COALESCE(
                  SUM(exchange_cash_rate) OVER (
                      PARTITION BY member_no
                      ORDER BY register_dt
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                  , 0) as current_cash
          ,lag(current_shoji) over (partition by user_id order by register_dt) as pre_shoji
          ,current_shoji 
          ,coalesce(coalesce(earn,0) - coalesce(exchange,0),0) as earn, coalesce(coalesce(spend,0) - coalesce(burnt,0)) as spend, exchange, exchange_cash_rate, burnt
      from df)


  select register_dt, user_id, member_no, pre_cash, current_cash, pre_shoji, current_shoji, earn, spend, exchange, exchange_cash_rate, burnt
  from fin_df 
  order by user_id, register_dt
)


