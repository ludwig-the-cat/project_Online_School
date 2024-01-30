/* Витрина для модели атрибуции Last Paid Click */
/* Создаём подзапрос в котором находим необходимые поля по условиям */
/* + добавляем row_number в разрезе id пользователей */
WITH temp AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id, 
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        row_number() over (PARTITION BY s.visitor_id ORDER BY s.visit_date DESC)
        AS rn
    /* Нумеруем users id, с сортировкой по совершившим последнюю покупку*/
    FROM sessions AS s
    LEFT JOIN leads AS l
    ON 
            s.visitor_id = l.visitor_id
                AND s.visit_date <= l.created_at
    WHERE s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
)

SELECT
    visitor_id,
    visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    lead_id,
    created_at,
    amount,
    closing_reason,
    status_id
FROM temp
WHERE
    rn = '1'
ORDER BY
    amount DESC NULLS LAST,
    visit_date ASC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 10;
