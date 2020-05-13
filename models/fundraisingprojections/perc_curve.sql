select row_number() over (partition by mailing_id order by hours_since_send desc) as row_number,
        hours.*
from
(SELECT hours_since_send,
        mailing_id,
        test_hours_since_send,
       (sum(raised_that_hour) over (partition by mailing_id order by hours_since_send rows unbounded preceding))/(sum(raised_that_hour) over (partition by mailing_id) ):: float as perc_curve
  from              
   (   SELECT umd.hours_since_send,
              tests.mailing_id,
              --hour long since mailings sent time
              tests.hours_since_send as test_hours_since_send,
              sum(umd.amount_converted) as raised_that_hour
       -- to get the test receivers
       FROM {{ ref('tests') }} 
       -- their earlier donations
       join mode.user_mailings_donations umd on tests.user_id = umd.user_id
       -- I exclude those weird cases with donation earlier than send
       where umd.hours_since_send >= 0 
       GROUP BY 1,2,3
       order by 1
    ) donations_by_hour
 order by 1 
 ) hours
-- we may not have the data for the exact number of hours the test was sent, so I need to join with the nearest smaller value (row_number above is used to take only one)
where test_hours_since_send >= hours_since_send   