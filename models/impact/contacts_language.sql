with contacts as  (
    select * from {{ ref('stg_contacts' )}}
),

languages as (
select
CASE
    WHEN upper(language) like upper('%eng%') then 'English'
    WHEN upper(language) like upper('%spa%') then 'Spanish'
    WHEN upper(language) like upper('%vie%') then 'Vietnamese'
    WHEN upper(language) like upper('%chi%') then 'Chinese'
    else language 
END as language,
    year(created_on) as years,
    count(distinct contact_id) as contacted
from contacts
group by language, years
)
select * from languages
where language is not null

/*language as (
    select 
        year(created_on) as years,
        language,
        count(distinct contact_id) as contacted
    from contacts
    group by years, language
)

select * from language*/