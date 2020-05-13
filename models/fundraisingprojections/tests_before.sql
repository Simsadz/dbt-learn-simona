select subq.*,
        -- get confidence intervals for donation rates
         p - se * 1.96 as low, 
         p as mid, 
         p + se * 1.96 as high 
from
(select grouped.*,
        -- for donation rate error calculation
        sqrt(p * (1 - p) / sent) as se 
from
(select mailing_id, 
        sum(donated)/count(*)::float as don_rate,
        count(*) as sent,
        (sum(donated) + 1.92) /(count(*) + 3.84)::float as p, -- Wald adjusted donation rate
        avg(amount_converted) as avg_gift,
        avg(case when amount_converted >= avg_gift - 2*se_gift and amount_converted <= avg_gift + 2*se_gift then amount_converted end) as avg_gift_no_outliers
    from
-- lists each test receiver and what they have donated in the past ( with avg gift and std deviation for remnoving outliers)
          (select tests.mailing_id, 
                  umd.user_id,
                  umd.donated,
                  umd.amount_converted,
                  avg(umd.amount_converted) over (partition by tests.mailing_id) as avg_gift,
                  stddev(umd.amount_converted) over (partition by tests.mailing_id) as se_gift
          from tests
          join mode.user_mailings_donations umd on umd.user_id = tests.user_id and umd.sent_at < tests.sent_at
          ) ungrouped
    group by 1 ) grouped
)subq    
