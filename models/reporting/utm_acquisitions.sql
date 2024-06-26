{{ config (
    alias = target.database + '_utm_acquisitions'
)}}

WITH previous_orders as 
    (SELECT DATE_TRUNC('day',date::date) as date, 'day' as date_granularity, NULL as customer_type, NULL as order_type, COUNT(*) as utm_acquisitions, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions') }}
    GROUP BY 1,2,3,4,6,7,8,9
    
    UNION ALL
    
    SELECT DATE_TRUNC('week',date::date) as date, 'week' as date_granularity, NULL as customer_type, NULL as order_type, COUNT(*) as utm_acquisitions, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions') }}
    GROUP BY 1,2,3,4,6,7,8,9
    
    UNION ALL
    
    SELECT DATE_TRUNC('month',date::date) as date, 'month' as date_granularity, NULL as customer_type, NULL as order_type, COUNT(*) as utm_acquisitions, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions') }}
    GROUP BY 1,2,3,4,6,7,8,9
    
    UNION ALL
    
    SELECT DATE_TRUNC('quarter',date::date) as date, 'quarter' as date_granularity, NULL as customer_type, NULL as order_type, COUNT(*) as utm_acquisitions, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions') }}
    GROUP BY 1,2,3,4,6,7,8,9
    
    UNION ALL
    
    SELECT DATE_TRUNC('year',date::date) as date, 'year' as date_granularity, NULL as customer_type, NULL as order_type, COUNT(*) as utm_acquisitions, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions') }}
    GROUP BY 1,2,3,4,6,7,8,9),
    
    first_orders as 
    (SELECT DATE_TRUNC('day',order_date::date) as date, 'day' as date_granularity, 'New' as customer_type, order_type, COUNT(*) as utm_acquisitions, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('google_sheets','gsheet_new_order') }}
    GROUP BY 1,2,3,4,6,7,8,9
    
    UNION ALL
    
    SELECT DATE_TRUNC('week',order_date::date) as date, 'week' as date_granularity, 'New' as customer_type, order_type, COUNT(*) as utm_acquisitions, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('google_sheets','gsheet_new_order') }}
    GROUP BY 1,2,3,4,6,7,8,9
    
    UNION ALL
    
    SELECT DATE_TRUNC('month',order_date::date) as date, 'month' as date_granularity, 'New' as customer_type, order_type, COUNT(*) as utm_acquisitions, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('google_sheets','gsheet_new_order') }}
    GROUP BY 1,2,3,4,6,7,8,9
    
    UNION ALL
    
    SELECT DATE_TRUNC('quarter',order_date::date) as date, 'quarter' as date_granularity, 'New' as customer_type, order_type, COUNT(*) as utm_acquisitions, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('google_sheets','gsheet_new_order') }}
    GROUP BY 1,2,3,4,6,7,8,9
    
    UNION ALL
    
    SELECT DATE_TRUNC('year',order_date::date) as date, 'year' as date_granularity, 'New' as customer_type, order_type, COUNT(*) as utm_acquisitions, utm_campaign, utm_content, utm_medium, utm_source
    FROM {{ source('google_sheets','gsheet_new_order') }}
    GROUP BY 1,2,3,4,6,7,8,9),
    
    current_orders as 
    (SELECT DATE_TRUNC('day',order_created_date_est::date) as date, 'day' as date_granularity, 
        CASE WHEN customer_type ~* 'first order' THEN 'New' WHEN customer_type ~* 'return order' THEN 'Returning' END as customer_type, 
        CASE WHEN subscription_orders = 1 THEN 'Subscription' ELSE 'One-Time' END as order_type, 
        COUNT(order_oms_order_id) as utm_acquisitions, utm_campaign,utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions_2024_with_utm_content') }}
    GROUP BY 1,2,3,4,6,7,8,9
    
    UNION ALL
    
    SELECT DATE_TRUNC('week',order_created_date_est::date) as date, 'week' as date_granularity, 
        CASE WHEN customer_type ~* 'first order' THEN 'New' WHEN customer_type ~* 'return order' THEN 'Returning' END as customer_type, 
        CASE WHEN subscription_orders = 1 THEN 'Subscription' ELSE 'One-Time' END as order_type,  
        COUNT(order_oms_order_id) as utm_acquisitions, utm_campaign,utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions_2024_with_utm_content') }}
    GROUP BY 1,2,3,4,6,7,8,9
    
    UNION ALL
    
    SELECT DATE_TRUNC('month',order_created_date_est::date) as date, 'month' as date_granularity, 
        CASE WHEN customer_type ~* 'first order' THEN 'New' WHEN customer_type ~* 'return order' THEN 'Returning' END as customer_type, 
        CASE WHEN subscription_orders = 1 THEN 'Subscription' ELSE 'One-Time' END as order_type, 
        COUNT(order_oms_order_id) as utm_acquisitions, utm_campaign,utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions_2024_with_utm_content') }}
    GROUP BY 1,2,3,4,6,7,8,9
    
    UNION ALL
    
    SELECT DATE_TRUNC('quarter',order_created_date_est::date) as date, 'quarter' as date_granularity,  
        CASE WHEN customer_type ~* 'first order' THEN 'New' WHEN customer_type ~* 'return order' THEN 'Returning' END as customer_type, 
        CASE WHEN subscription_orders = 1 THEN 'Subscription' ELSE 'One-Time' END as order_type, 
        COUNT(order_oms_order_id) as utm_acquisitions, utm_campaign,utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions_2024_with_utm_content') }}
    GROUP BY 1,2,3,4,6,7,8,9
    
    UNION ALL
    
    SELECT DATE_TRUNC('year',order_created_date_est::date) as date, 'year' as date_granularity,  
        CASE WHEN customer_type ~* 'first order' THEN 'New' WHEN customer_type ~* 'return order' THEN 'Returning' END as customer_type, 
        CASE WHEN subscription_orders = 1 THEN 'Subscription' ELSE 'One-Time' END as order_type, 
        COUNT(order_oms_order_id) as utm_acquisitions, utm_campaign,utm_content, utm_medium, utm_source
    FROM {{ source('gsheet_raw','utm_acquisitions_2024_with_utm_content') }}
    GROUP BY 1,2,3,4,6,7,8,9)
    
    SELECT * FROM previous_orders
    UNION ALL
    SELECT * FROM first_orders
    UNION ALL
    SELECT * FROM current_orders
