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
WITH vk_and_yandex AS (
    SELECT
        TO_CHAR(
            campaign_date, 'YYYY-MM-DD'
        )
        AS campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM
        vk_ads
    GROUP BY
        TO_CHAR(
            campaign_date, 'YYYY-MM-DD'
        ),
        utm_source,
        utm_medium,
        utm_campaign
    UNION ALL
    SELECT
        TO_CHAR(
            campaign_date, 'YYYY-MM-DD'
        )
        AS campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM
        ya_ads
    GROUP BY
        TO_CHAR(
            campaign_date, 'YYYY-MM-DD'
        ),
        utm_source,
        utm_medium,
        utm_campaign
),

/* Создаём подзапрос в котором соединяем таблицы сессий и лидов */
last_paid_users AS (
    SELECT
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        s.visitor_id,
        l.lead_id,
        l.status_id,
        l.closing_reason,
        l.amount,
        LOWER(s.source) AS utm_source,
        TO_CHAR(
            s.visit_date, 'YYYY-MM-DD'
        )
        AS visit_date,
        ROW_NUMBER() OVER (
            PARTITION BY s.visitor_id
            ORDER BY s.visit_date DESC
        ) AS rn
    /* Нумеруем пользователей совершивших последний платный клик */
    FROM
        sessions AS s
    LEFT JOIN leads AS l
        ON
            s.visitor_id = l.visitor_id
            AND s.visit_date <= l.created_at
    WHERE
        s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
        AND s.source IN ('vk', 'yandex')
/* Находим пользователей только с платными кликами */
)

SELECT
    /* В основном запросе находим необходимые по условию поля */
    lpu.visit_date,
    lpu.utm_source,
    lpu.utm_medium,
    lpu.utm_campaign,
    COUNT(lpu.visitor_id) AS visitors_count,
    SUM(vy.total_cost) AS total_cost,
    COUNT(lpu.lead_id) AS leads_count,
    COUNT(
        CASE
            WHEN
                lpu.status_id = '142'
                THEN '1'
        END
    ) AS purchase_count,
    SUM(
        CASE
            WHEN
                lpu.status_id = '142'
                THEN lpu.amount
        END
    ) AS revenue
FROM
    last_paid_users AS lpu
LEFT JOIN vk_and_yandex AS vy
    /* Соединяем по utm-меткам и дате проведения кампании */
    ON
        lpu.utm_source = vy.utm_source
        AND lpu.utm_medium = vy.utm_medium
        AND lpu.utm_campaign = vy.utm_campaign
        AND lpu.visit_date = vy.campaign_date
WHERE
    lpu.rn = '1'
    AND lpu.utm_source IN ('vk', 'yandex')
/* Оставляем только пользователей с последним платным кликом */
GROUP BY
    lpu.visit_date,
    lpu.utm_source,
    lpu.utm_medium,
    lpu.utm_campaign
ORDER BY
    lpu.visit_date,
    lpu.utm_source,
    lpu.utm_medium,
    SUM(vy.total_cost) DESC,
    SUM(
        CASE
            WHEN
                lpu.status_id = '142'
                THEN lpu.amount
        END
    ) DESC NULLS LAST;


