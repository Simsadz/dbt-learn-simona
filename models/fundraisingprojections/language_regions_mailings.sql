SELECT cm.id AS mailing_id,
            -- regions, one mailing can have a few! then I get both
            CASE WHEN cmt1.tag_id = 953 THEN 'Global'
                 WHEN cmt1.tag_id = 954 THEN 'Cont. Europe' -- to add france, germany?
                 WHEN cmt1.tag_id = 967 THEN 'Canada'
                 WHEN cmt1.tag_id = 966 THEN 'United States'
                 WHEN cmt1.tag_id = 968 THEN 'United Kingdom'
                 WHEN cmt1.tag_id = 1140 THEN 'New Zealand'
                 WHEN cmt1.tag_id = 969 THEN 'Australia'
                 WHEN cmt1.tag_id = 1668 THEN 'RoW' -- other national plus row
                 --if its not tagged i assume its global
                 else 'Global'
            END AS region,
            -- language better from tags not from page_id, otherwise no language if no donations yet
            CASE
                 WHEN cmt3.tag_id = 1130 THEN 'French'
                 WHEN cmt3.tag_id = 1132 THEN 'German'
                 WHEN cmt3.tag_id = 2107 THEN 'Spanish'
                 ELSE 'English' END AS language
       FROM ak_sumofus.core_mailing cm
       -- Region tags
       -- left join to take even those without regiona tags - then I assume its global
       LEFT JOIN ak_sumofus.core_mailing_tags cmt1 ON cmt1.mailing_id = cm.id AND cmt1.tag_id IN (969,967,954,953,968,966,1140, 1668)  -- region tags
        -- left join to take even those without language tags - then I assume its english
       LEFT JOIN ak_sumofus.core_mailing_tags cmt3 ON cmt3.mailing_id = cm.id AND cmt3.tag_id IN (1130, 1132, 1282, 2107)  -- language tags