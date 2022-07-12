with msgs as  (
    select * from {{ ref('msgs_contact_info' )}}
),

languages as (
    select
        count(msg_id) as Messages,
        count(distinct contact_id) as Contacts,
        CASE
            WHEN upper(lang) like upper('%English%') then 'English'
            WHEN upper(lang) like upper('%Spanish%') then 'Spanish'
            else 'other languages' 
        END as language,
        year_created as years
    from msgs
    where sent_on is not null
    and direction = 'O'
    group by language, years
),

totals as(
    select 
        count(msg_id) as total,
        count(distinct contact_id) as Total_contacts,
        year_created
    from msgs
    where sent_on is not null
    and direction = 'O'
    group by year_created
),

final as (
    select 
        languages.*,
        totals.total,
        totals.Total_contacts
    from languages
    left join totals on totals.year_created = languages.years
)

select 
    *,
    Messages/total * 100 as Percentage,
    Contacts/Total_contacts *100 as Percentage_Contacts
from final