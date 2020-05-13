select subq2.*,
        -- get confidence intervals for donation rates
         p - se * 1.96 as low, 
         p as mid, 
         p + se * 1.96 as high 
from
(select subq.*,
        -- for error calculation
        sqrt(p * (1 - p) / fundraisers_sent) as se 
from
(select c.mailing_id, 
       sum(list_size) as list_size, 
       sum(region_non_donors) as region_non_donors,
       sum(fundraisers_sent) as fundraisers_sent,
       sum(gifts)/sum(fundraisers_sent)::float as don_rate,
       -- donation rate on full listers
       sum(fl_gifts)/sum(fl_fundraisers_sent)::float as fl_don_rate,
       -- non donors donation rate 
       sum(nd_gifts)/sum(nd_fundraisers_sent)::float as nd_don_rate,
       (sum(gifts) + 1.92) /(sum(fundraisers_sent) + 3.84)::float as p, -- Wald adjusted donation rate
       sum(raised)/sum(gifts)::float as avg_gift,
       -- avg gift on full listers
       sum(fl_raised)/sum(fl_gifts)::float as fl_avg_gift,
       -- non donors average gift
       sum(fl_raised)/sum(fl_gifts)::float as nd_avg_gift,
       ---recalculate avg gift, in case there are a few regions
       sum(avg_gift_no_outliers*gifts)/sum(gifts) as avg_gift_no_outliers
FROM
--- just to get mailings and regions, might be a few regions to a mailing       
(select mailing_id, mailing_region as region, mailing_language as language
from {{ ref('tests') }} t
group by 1,2,3) c
join mode.regions_fundraising r ON (case when c.region = 'Global' then c.language = r.language else (c.region = r.region and c.language = r.language) end )
group by 1)subq
)subq2
