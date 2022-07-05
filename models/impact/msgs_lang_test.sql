with contacts as  (
    select * from {{ ref('stg_contacts' )}}
),

languages as (
    select
    CASE
        WHEN upper(contacts.language) like upper('%eng%') then 'English'
        WHEN contacts.language is null then 'English'
        WHEN upper(contacts.language) like upper('%spa%') then 'Spanish'
        WHEN upper(contacts.language) like upper('%vie%') then 'Vietnamese'
        WHEN upper(contacts.language) like upper('%chi%') then 'Chinese'
        else contacts.language
    END as lang, 
        year(date(contacts.created_on)) as year_created,
        month(date(contacts.created_on)) as months,
        (contacts.contact_id) as contact_id,
        date(created_on) as date,
        contacts.org_id as org_id
    from contacts
),

messages as(
    select *
        from airbyte_db.ccl.msgs_msg
    where airbyte_db.ccl.msgs_msg.direction = 'O'
    and airbyte_db.ccl.msgs_msg.error_count = 0
),

final as(
    select
        languages.lang as lang,
        languages.year_created,
        messages.sent_on,
        messages.id,
        messages.text,
        messages.contact_id,
        CASE
            WHEN messages.EXTERNAL_ID like '%MM%' then 'MMS'
            WHEN messages.EXTERNAL_ID like '%SM%' then 'SMS'
            else messages.EXTERNAL_ID
        END as msg_type,
        languages.date,
        languages.months,
        messages.org_id
    from messages
    left join languages on messages.contact_id = languages.contact_id
)

select final.lang, final.msg_type, final.months, count(final.id) from final
    where org_id = 14
    and lang != ' '
    and date < '2021-12-31' and date >'2021-07-01'
group by final.lang, final.months, final.msg_type
