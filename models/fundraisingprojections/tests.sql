select aum.mailing_id as mailing_id,
       cm.notes,
       progress,
       cm.started_at,
       Language_regions_mailings.region as mailing_region, 
       Language_regions_mailings.language as mailing_language,
       aum.user_id, 
       aum.created_at as sent_at,
       amount_converted,
       avg(amount_converted) over (partition by aum.mailing_id) as avg_gift,
       --standard deviation
       stddev(amount_converted) over (partition by aum.mailing_id) as se_gift,
       case when amount_converted is not null then 1 else 0 end as donated,
       DATEDIFF(hours,cm.started_at,getdate())+1 AS hours_since_send,
       case when not exists (select * from mode.transactions t where t.user_id = aum.user_id and t.created_at < aum.created_at) then 1 else 0 end as non_donor
FROM ak_sumofus.core_usermailing aum
join ak_sumofus.core_mailing cm on cm.id = aum.mailing_id and cm.started_at >= getdate() - interval '1 month' and cm.recurring_schedule_id is null
-- only tests and mvps
JOIN ak_sumofus.core_mailing_tags cmt1 ON cmt1.mailing_id = cm.id AND cmt1.tag_id IN (948,949) -- tests and mvps
       -- just fundraisers
--only fundraisers
join ak_sumofus.core_mailing_tags cmt ON cmt.mailing_id = cm.id AND cmt.tag_id = 972
join {{ ref('language_regions_mailings') }} on language_regions_mailings.mailing_id = cm.id 
left join mode.transactions mt on mt.mailing_id = aum.mailing_id and mt.user_id = aum.user_id and trans_type <> 'recur_ongoing'
