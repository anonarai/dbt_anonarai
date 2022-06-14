with msgs as(
    select *
    from airbyte_db.ccl.msgs_msg
),

contacts as  (
    select * from {{ ref('stg_contacts' )}}
),

urns as  (
    select * from {{ ref('stg_urns' )}}
),

msg_urns as (
    select 
        msgs.id as msg_id,
        msgs.text as message,
        msgs.org_id,
        msgs.sent_on,
        msgs.error_count,
        msgs.direction,
        msgs.msg_count,
        msgs.channel_id,
        msgs.next_attempt,
        msgs.response_to_id,
        msgs.uuid as msg_uuid,
        msgs.contact_urn_id as contact_urn,
        msgs.contact_id as contact_id,
        year(date(msgs.sent_on)) as year_created,
        month(date(msgs.sent_on)) as months,
        CASE
            WHEN msgs.EXTERNAL_ID like '%MM%' then 'MMS'
            WHEN msgs.EXTERNAL_ID like '%SM%' then 'SMS'
            else msgs.EXTERNAL_ID
        END as msg_type,
        urns.contact,
        urns.org_name,
        urns.channel
    from msgs

    left join urns on msgs.contact_urn_id = urns.contact_urn
),

msgs_full_contacts as (
    select 
        msg_urns.*,
        contacts.contact_name,
        contacts.created_on as contact_created_date,
        contacts.last_seen_on as contact_last_seen,
        contacts.fields,
        contacts.is_active as is_contact_active,
        CASE
            WHEN upper(contacts.language) like upper('%eng%') then 'English'
            WHEN contacts.language is null then 'English'
            WHEN upper(contacts.language) like upper('%spa%') then 'Spanish'
            WHEN upper(contacts.language) like upper('%vie%') then 'Vietnamese'
            WHEN upper(contacts.language) like upper('%chi%') then 'Chinese'
            else contacts.language
        END as lang 
    from msg_urns

    left join contacts on msg_urns.contact_id = contacts.contact_id
)
select * from msgs_full_contacts
