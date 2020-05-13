select subq2.*,
        -- get confidence intervals for donation rates
         p - se * 1.96 as low, 
         p as mid, 
         p + se * 1.96 as high 
from
(select subq.*,
        -- for error calculation
        sqrt(p * (1 - p) / received) as se 
from
(select mailing_id, started_at, notes, progress, hours_since_send,
        -- number of previously non-donors on that test
        sum(non_donor) as non_donors,
        -- no regions, cause there might be very few donations (say in 2 hours) and we don't want to be spliting it all by region
        -- umd.region, umd.language, 
        sum(amount_converted) as test_raised,
        sum(donated) as test_gifts,
        avg_gift, 
        avg(case when amount_converted >= avg_gift - 2*se_gift and amount_converted <= avg_gift + 2*se_gift then amount_converted end) as avg_gift_no_outliers,
        count(distinct user_id) as received,
        sum(donated)/count(distinct user_id):: float as don_rate,
        (sum(donated) + 1.92) /(count(distinct user_id) + 3.84)::float as p -- Wald adjusted donation rate
FROM {{ ref('tests') }}
group by 1,2,3,4,5, se_gift,avg_gift) subq
)subq2
