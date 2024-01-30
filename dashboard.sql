/* Запрос находит кол-во пользователей и лидов, заходящих на сайт. */
/* Показывает каналы, по которым они приходят в разрезе дней/недель/месяцев. */
/* Дополнительно можно найти lc и lcr для каждого канала */
WITH advert AS (
    SELECT
        s.medium AS utm_medium,
        s.visitor_id,
        l.lead_id,
        TO_CHAR(s.visit_date, 'YYYY-MM-DD') AS visit_date,
        TO_CHAR(s.visit_date, 'day') AS day_of_week,
        TO_CHAR(s.visit_date, 'W') AS number_of_week,
        TO_CHAR(s.visit_date, 'Month') AS month_name,
        CASE WHEN l.amount != '0' OR NULL THEN '1' END AS amount
    FROM sessions AS s
    LEFT JOIN
        leads AS l ON
        s.visitor_id = l.visitor_id
        AND s.visit_date <= l.created_at
)

SELECT
    visit_date,
    day_of_week,
    number_of_week,
    month_name,
    utm_medium,
    COUNT(DISTINCT visitor_id) AS visitors_count,
    COUNT(DISTINCT lead_id) AS leads_count,
    COUNT(amount) AS customers_count,
    ROUND(
        CAST(
            CAST(
                COUNT(lead_id) AS FLOAT) / NULLIF(
                CAST(
                    COUNT(DISTINCT visitor_id) AS FLOAT
                ), 0
            ) * 100 AS NUMERIC
        ), 2
    )
    AS lcr,
    ROUND(
        CAST(
            CAST(
                COUNT(amount) AS FLOAT) / NULLIF(
                CAST(
                    COUNT(lead_id) AS FLOAT
                ), 0
            ) * 100 AS NUMERIC
        ), 2
    )
    AS lc
FROM advert
GROUP BY
    visit_date,
    day_of_week,
    number_of_week,
    month_name,
    utm_medium
ORDER BY
    visit_date;
/* Общая конверсия из клика в лид и из лида в оплату */
WITH conv_rate AS (
    SELECT
        s.visitor_id,
        l.lead_id,
        CASE WHEN l.amount != '0' OR NULL THEN '1' END AS amount
    FROM sessions AS s
    LEFT JOIN
        leads AS l ON
        s.visitor_id = l.visitor_id
        AND s.visit_date <= l.created_at
    WHERE s.medium != 'organic'
)

SELECT
    ROUND(
        CAST(
            CAST(
                COUNT(lead_id) AS FLOAT) / CAST(
                COUNT(DISTINCT visitor_id) AS FLOAT
            ) * 100 AS NUMERIC
        ), 2
    )
    AS lcr,
    ROUND(
        CAST(
            CAST(
                COUNT(amount) AS FLOAT) / CAST(
                COUNT(lead_id) AS FLOAT
            ) * 100 AS NUMERIC
        ), 2
    )
    AS lc
FROM conv_rate;

-- Запрос находит стоимость рекламы в различных каналах и доходы.
WITH main1 AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        vk.daily_spent AS ads_cost,
        LOWER(s.source) AS utm_source
    FROM sessions AS s
    LEFT JOIN
        vk_ads AS vk ON
        s.source = vk.utm_source
        AND s.medium = vk.utm_medium
        AND s.campaign = vk.utm_campaign
    LEFT JOIN
        ya_ads AS ya ON
        s.source = ya.utm_source
        AND s.medium = ya.utm_medium
        AND s.campaign = ya.utm_campaign
),

main2 AS (
    SELECT
        m1.utm_medium,
        m1.utm_campaign,
        m1.ads_cost,
        LOWER(m1.utm_source) AS utm_source,
        CASE WHEN l.amount = '0' THEN NULL ELSE l.amount END AS revenue
    FROM main1 AS m1
    LEFT JOIN
        leads AS l ON
        m1.visitor_id = l.visitor_id
        AND m1.visit_date <= l.created_at
)

SELECT
    utm_medium,
    SUM(ads_cost) AS adv_costs,
    SUM(revenue) AS revenue
FROM main2
GROUP BY
    utm_medium;

-- За сколько дней с момента перехода по рекламе закрывается 90% лидов.
WITH registration_date AS (
    SELECT
        visitor_id,
        visit_date AS first_visit_date,
        source,
        medium,
        campaign,
        ROW_NUMBER() OVER (PARTITION BY visitor_id ORDER BY visit_date ASC)
        AS rn
    FROM sessions
),

main AS (
    SELECT
        rd.visitor_id,
        rd.first_visit_date,
        l.lead_id,
        l.created_at AS lead_date,
        rd.source,
        rd.medium,
        rd.campaign,
        l.amount
    FROM registration_date AS rd
    LEFT JOIN
        leads AS l ON
        rd.visitor_id = l.visitor_id
        AND rd.first_visit_date <= l.created_at
    WHERE
        rd.rn = '1'
        AND l.closing_reason = 'Успешная продажа'
)

SELECT
    medium,
    ROUND(
        CAST(
            AVG(
                EXTRACT(DAY FROM lead_date - first_visit_date)
            ) AS NUMERIC
        ), 0
    )
    AS lifetime,
    AVG(amount) AS avg_amount,
    AVG(
        EXTRACT(DAY FROM lead_date - first_visit_date)
    ) * AVG(amount)
    AS ltv
FROM main
GROUP BY medium
ORDER BY
    lifetime;

WITH advert AS (
    /* За сколько закрывается 90 процентов сделок по рекламным кампаниям? */
    SELECT
        s.visitor_id,
        l.lead_id,
        TO_CHAR(s.visit_date, 'YYYY-MM-DD') AS visit_date,
        TO_CHAR(l.created_at, 'YYYY-MM-DD') AS created_at,
        CASE WHEN l.amount != '0' OR NULL THEN '1' END AS amount
    FROM sessions AS s
    LEFT JOIN
        leads AS l ON
        s.visitor_id = l.visitor_id
        AND s.visit_date <= l.created_at
)

SELECT
    visit_date,
    utm_medium,
    lead_id
FROM advert
ORDER BY visit_date;


-- Основные метрики:
-- cpu = total_cost / visitors_count
-- cpl = total_cost / leads_count
-- cppu = total_cost / purchases_count
-- roi = (revenue - total_cost) / total_cost * 100%
-- Взял скрипт из файла aggregate_last_paid_click
-- и составил сводную таблицу с метриками 
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