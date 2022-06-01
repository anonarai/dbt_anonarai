with msgs as  (
    select * from {{ ref('stg_outgoing' )}}
),

total_year as (
    select 
        year(sent_on) as year,
        count(distinct contact_id) as all_contacts
    from msgs
    group by year
    ),
sms_year as (
    select 
        year(sent_on) as year,
        count(distinct contact_id) as mobile
    from msgs
        where schemes = '["tel"]'
    group by year
),
web_year as (
    select 
        year(sent_on) as year,
        count(distinct contact_id) as web
    from msgs
        where schemes = '["ext"]'
    group by year
),
fb as (
    select 
        year(sent_on) as year,
        count(distinct contact_id) as fb
    from msgs
        where schemes = '["facebook"]'
    group by year
),
wp as (
    select 
        year(sent_on) as year,
        count(distinct contact_id) as whatsapp
    from msgs
        where schemes = '["whatsapp"]'
    group by year
),
final as(
    select * from total_year
    full outer join sms_year using(year)
    full outer join web_year using(year)
    full outer join fb using(year)
    full outer join wp using(year)
)

select * from final
