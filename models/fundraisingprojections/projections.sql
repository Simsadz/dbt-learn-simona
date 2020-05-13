select tests_now.mailing_id,
       tests_now.started_at as "Test Date",
       tests_now.hours_since_send as "Hours since send",
       tests_now.notes,
       tests_now.progress as "Size of Test",
       tests_now.test_gifts AS "# gifts",
       tests_now.test_raised AS "$ raised",
       list_size as "Full list size",
       (tests_now.avg_gift * regions.avg_gift/tests_before.avg_gift)*(tests_now.mid * regions.mid/tests_before.mid)*list_size/perc_curve + (nd_avg_gift * nd_don_rate * region_non_donors) as "Total projected $ raised",
       (tests_now.don_rate * regions.mid/tests_before.mid)*list_size/perc_curve + (nd_don_rate * non_donors) as "Total projected # gifts",
       regions.fl_avg_gift * regions.fl_don_rate * list_size as "Min Value"

from

-- RAISED FROM TESTS NOW
{{ ref('tests_now') }}

JOIN
-- RAISED FROM TEST PEOPLE BEFORE
(( ref('tests_before') }} on tests_before.mailing_id = tests_now.mailing_id

JOIN
-- FULL list REGIONS all together () might be a few tags, so need to sum)      

{{ ref('regions')}} on regions.mailing_id = tests_now.mailing_id


JOIN
-- hourly curve only among test people, for each test mailing separate       
 {{ ref('perc_curve') }} on perc_curve.mailing_id = tests_now.mailing_id
 
-- important to have only one closest value form hourly curve
where row_number = 1
order by tests_now.hours_since_send 



