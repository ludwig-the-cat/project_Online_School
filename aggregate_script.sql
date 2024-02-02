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