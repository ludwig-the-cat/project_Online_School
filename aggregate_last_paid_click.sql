/*Делаем CTE чтобы агрегировать посетелей по последнему клику*/
with last_click as (
    select
        visitor_id,
        MAX(visit_date) as visit_date
    from sessions
    where
        LOWER(medium) in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
    group by visitor_id
),
/*создаем CTE для агрегации по дате создания, utm_меткам и лидам и их статусу*/ 
last_paid_click as (
    select
        l_c.visitor_id,
        l_c.visit_date,
        s.source as utm_source,
        s.medium as utm_medium,
        s.campaign as utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id
    from last_click as l_c
    inner join sessions as s
        on
            l_c.visitor_id = s.visitor_id
            and l_c.visit_date = s.visit_date
    left join leads as l
        on
            l_c.visitor_id = l.visitor_id
            and l_c.visit_date <= l.created_at
),
/*агрегация по Utm метакам в рамках компаний VK и Ya*/
ads as (
    select
        TO_CHAR(campaign_date, 'yyyy-mm-dd') as campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) as total_cost
    from vk_ads
    group by
        TO_CHAR(campaign_date, 'yyyy-mm-dd'),
        utm_source,
        utm_medium,
        utm_campaign
    union all
    select
        TO_CHAR(campaign_date, 'yyyy-mm-dd') as campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) as total_cost
    from ya_ads
    group by
        TO_CHAR(campaign_date, 'yyyy-mm-dd'),
        utm_source,
        utm_medium,
        utm_campaign
),

agg_tab as (
    select
        utm_source,
        utm_medium,
        utm_campaign,
        TO_CHAR(visit_date, 'yyyy-mm-dd') as visit_date,
        COUNT(distinct visitor_id) as visitors_count,
        COUNT(lead_id) as leads_count,
        COUNT(lead_id) filter (
            where
            closing_reason = 'Успешно реализовано'
            or status_id = 142
        ) as purchases_count,
        SUM(amount) as revenue
    from last_paid_click
    group by
        TO_CHAR(visit_date, 'yyyy-mm-dd'),
        utm_source,
        utm_medium,
        utm_campaign
)

select
    ag.visit_date,
    ag.visitors_count,
    ag.utm_source,
    ag.utm_medium,
    ag.utm_campaign,
    ads.total_cost,
    ag.leads_count,
    ag.purchases_count,
    ag.revenue
from agg_tab as ag
left join ads
    on
        ag.visit_date = ads.campaign_date
        and ag.utm_source = ads.utm_source
        and ag.utm_medium = ads.utm_medium
        and ag.utm_campaign = ads.utm_campaign
order by
    ag.revenue desc nulls last,
    ag.visit_date asc,
    ag.visitors_count desc,
    ag.utm_source asc,
    ag.utm_medium asc,
    ag.utm_campaign asc
    limit 15;