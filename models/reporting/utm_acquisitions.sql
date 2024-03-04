{{ config (
    alias = target.database + '_utm_acquisitions'
)}}

WITH previous_orders as 
    (SELECT DATE_TRUNC('day',date::date) as date, 'day' as date_granularity, COUNT(*) as utm_acquisitions, 0 as utm_first_orders, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions') }}
    GROUP BY 1,2,5,6,7,8
    
    UNION ALL
    
    SELECT DATE_TRUNC('week',date::date) as date, 'week' as date_granularity, COUNT(*) as utm_acquisitions, 0 as utm_first_orders, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions') }}
    GROUP BY 1,2,5,6,7,8
    
    UNION ALL
    
    SELECT DATE_TRUNC('month',date::date) as date, 'month' as date_granularity, COUNT(*) as utm_acquisitions, 0 as utm_first_orders, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions') }}
    GROUP BY 1,2,5,6,7,8
    
    UNION ALL
    
    SELECT DATE_TRUNC('quarter',date::date) as date, 'quarter' as date_granularity, COUNT(*) as utm_acquisitions, 0 as utm_first_orders, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions') }}
    GROUP BY 1,2,5,6,7,8
    
    UNION ALL
    
    SELECT DATE_TRUNC('year',date::date) as date, 'year' as date_granularity, COUNT(*) as utm_acquisitions, 0 as utm_first_orders, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions') }}
    GROUP BY 1,2,5,6,7,8),
    
    first_orders as 
    (SELECT DATE_TRUNC('day',order_date::date) as date, 'day' as date_granularity, 0 as utm_acquisitions, COUNT(*) as utm_first_orders, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('google_sheets','gsheet_new_order') }}
    GROUP BY 1,2,5,6,7,8
    
    UNION ALL
    
    SELECT DATE_TRUNC('week',order_date::date) as date, 'week' as date_granularity, 0 as utm_acquisitions, COUNT(*) as utm_first_orders, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('google_sheets','gsheet_new_order') }}
    GROUP BY 1,2,5,6,7,8
    
    UNION ALL
    
    SELECT DATE_TRUNC('month',order_date::date) as date, 'month' as date_granularity, 0 as utm_acquisitions, COUNT(*) as utm_first_orders, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('google_sheets','gsheet_new_order') }}
    GROUP BY 1,2,5,6,7,8
    
    UNION ALL
    
    SELECT DATE_TRUNC('quarter',order_date::date) as date, 'quarter' as date_granularity, 0 as utm_acquisitions, COUNT(*) as utm_first_orders, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('google_sheets','gsheet_new_order') }}
    GROUP BY 1,2,5,6,7,8
    
    UNION ALL
    
    SELECT DATE_TRUNC('year',order_date::date) as date, 'year' as date_granularity, 0 as utm_acquisitions, COUNT(*) as utm_first_orders, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('google_sheets','gsheet_new_order') }}
    GROUP BY 1,2,5,6,7,8),
    
    current_orders as 
    (SELECT DATE_TRUNC('day',date::date) as date, 'day' as date_granularity, COUNT(*) as utm_acquisitions, 0 as utm_first_orders, utm_campaign, NULL as utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions_2024') }}
    GROUP BY 1,2,5,6,7,8
    
    UNION ALL
    
    SELECT DATE_TRUNC('week',date::date) as date, 'week' as date_granularity, COUNT(*) as utm_acquisitions, 0 as utm_first_orders, utm_campaign, NULL as utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions_2024') }}
    GROUP BY 1,2,5,6,7,8
    
    UNION ALL
    
    SELECT DATE_TRUNC('month',date::date) as date, 'month' as date_granularity, COUNT(*) as utm_acquisitions, 0 as utm_first_orders, utm_campaign, NULL as utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions_2024') }}
    GROUP BY 1,2,5,6,7,8
    
    UNION ALL
    
    SELECT DATE_TRUNC('quarter',date::date) as date, 'quarter' as date_granularity, COUNT(*) as utm_acquisitions, 0 as utm_first_orders, utm_campaign, NULL as utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions_2024') }}
    GROUP BY 1,2,5,6,7,8
    
    UNION ALL
    
    SELECT DATE_TRUNC('year',date::date) as date, 'year' as date_granularity, COUNT(*) as utm_acquisitions, 0 as utm_first_orders, utm_campaign, NULL as utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions_2024') }}
    GROUP BY 1,2,5,6,7,8)
    
    SELECT * FROM previous_orders
    UNION ALL
    SELECT * FROM first_orders
    UNION ALL
    SELECT * FROM current_orders
