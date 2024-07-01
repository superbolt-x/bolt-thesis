{{ config (
    alias = target.database + '_blended_performance'
)}}

with data as ( 
select date, date_granularity, utm_campaign, SUM(utm_acquisitions) as utm_acquisitions, sum(case when customer_type = 'New' THEN utm_acquisitions END) as utm_first_orders
FROM {{ source('reporting','utm_acquisitions') }}
WHERE utm_source ~* 'google'
group by 1,2,3
order by 3,1
), 

final as(
    SELECT adw.date, adw.date_granularity, campaign_name, campaign_targeting, campaign_type_custom, 
        COALESCE(SUM(spend),0) as spend, COALESCE(SUM(utm_acquisitions),0) as utm_acquisitions, COALESCE(SUM(utm_first_orders),0) as utm_first_orders
    FROM {{ source('reporting','googleads_campaign_performance') }} adw
    LEFT JOIN data utm 
        ON adw.date = utm.date 
        AND adw.date_granularity = utm.date_granularity 
        AND adw.campaign_id = utm.utm_campaign
    
    GROUP BY 1,2,3,4,5
    order by 1)

select *
from final
