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
        year(contacts.created_on) as year_created,
        (contacts.contact_id) as contact_id,
        contacts.org_id as org_id
    from contacts
),

messages as(
    select *
        from pc_fivetran_db.pg_public.msgs_msg
    where pc_fivetran_db.pg_public.msgs_msg.direction = 'O'
    and pc_fivetran_db.pg_public.msgs_msg.error_count = 0
),

final as(
    select
        languages.lang as lang,
        languages.year_created,
        messages.sent_on,
        messages.id,
        messages.text,
        messages.contact_id,
        messages.org_id
    from messages
    left join languages on messages.contact_id = languages.contact_id
)
select final.lang, final.year_created, count(final.id) from final
    where org_id = 14
    and lang != ' '
group by final.lang, final.year_created
order by year_created desc
