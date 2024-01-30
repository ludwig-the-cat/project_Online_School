/*Делаем CTE чтобы агрегировать посетелей по последнему клику*/
WITH last_click AS (
    SELECT
        visitor_id,
        MAX(visit_date) AS visit_date
    FROM sessions
    WHERE
        LOWER(medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
    GROUP BY visitor_id
),
/*создаем CTE для агрегации по дате создания, utm_меткам и лидам и их статусу*/ 
last_paid_click AS (
    SELECT
        l_c.visitor_id,
        l_c.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id
    FROM last_click AS l_c
    INNER JOIN sessions AS s
        ON
            l_c.visitor_id = s.visitor_id
            AND l_c.visit_date = s.visit_date
    LEFT JOIN leads AS l
        ON
            l_c.visitor_id = l.visitor_id
            AND l_c.visit_date <= l.created_at
),
/*агрегация по Utm метакам в рамках компаний VK и Ya*/
ads AS (
    SELECT
        TO_CHAR(campaign_date, 'yyyy-mm-dd') AS campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM vk_ads
    GROUP BY
        TO_CHAR(campaign_date, 'yyyy-mm-dd'),
        utm_source,
        utm_medium,
        utm_campaign
    UNION ALL
    SELECT
        TO_CHAR(campaign_date, 'yyyy-mm-dd') as campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) as total_cost
    FROM ya_ads
    GROUP BY
        TO_CHAR(campaign_date, 'yyyy-mm-dd'),
        utm_source,
        utm_medium,
        utm_campaign
),

agg_tab AS (
    SELECT
        utm_source,
        utm_medium,
        utm_campaign,
        TO_CHAR(visit_date, 'yyyy-mm-dd') AS visit_date,
        COUNT(DISTINCT visitor_id) AS visitors_count,
        COUNT(lead_id) AS leads_count,
        COUNT(lead_id) filter (
            WHERE
            closing_reason = 'Успешно реализовано'
            OR status_id = 142
        ) AS purchases_count,
        SUM(amount) AS revenue
    FROM last_paid_click
    GROUP BY
        TO_CHAR(visit_date, 'yyyy-mm-dd'),
        utm_source,
        utm_medium,
        utm_campaign
)

SELECT
    ag.visit_date,
    ag.visitors_count,
    ag.utm_source,
    ag.utm_medium,
    ag.utm_campaign,
    ads.total_cost,
    ag.leads_count,
    ag.purchases_count,
    ag.revenue
FROM agg_tab AS ag
LEFT JOIN ads
    ON
        ag.visit_date = ads.campaign_date
        AND ag.utm_source = ads.utm_source
        AND ag.utm_medium = ads.utm_medium
        AND ag.utm_campaign = ads.utm_campaign
ORDER BY
    ag.revenue DESC nulls LAST,
    ag.visit_date ASC,
    ag.visitors_count DESC,
    ag.utm_source ASC,
    ag.utm_medium ASC,
    ag.utm_campaign ASC
    LIMIT 15;
