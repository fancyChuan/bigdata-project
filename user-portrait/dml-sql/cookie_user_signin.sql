INSERT OVERWRITE TABLE ods.cookie_user_signin PARTITION (data_date = '${data_date}')
SELECT t.*
FROM (
    SELECT userid,
        cookieid,
        from_unixtime(eventtime,'yyyyMMdd') as signdate
    FROM ods.page_event_log      -- 埋点表
    WHERE data_date = '${data_date}'
    UNION ALL
    SELECT userid,
        cookieid,
        from_unixtime(viewtime,'yyyyMMdd') as signdate
    FROM ods.page_view_log   -- 访问日志表
    WHERE data_date = '${data_date}'
    ) t
;