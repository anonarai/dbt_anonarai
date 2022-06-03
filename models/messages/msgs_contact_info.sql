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
        msgs.contact_urn_id as contact_urn,
        msgs.contact_id as contact_id,
        msgs.org_id,
        msgs.sent_on,
        msgs.error_count,
        msgs.direction,
        msgs.msg_count,
        msgs.channel_id,
        msgs.next_attempt,
        msgs.response_to_id,
        msgs.uuid,
        urns.contact,
        urns.org_name,
        urns.channel
    from msgs

    left join urns on msgs.contact_urn_id = urns.contact_urn
),

msgs_full_contacts as (
    select 
        *,
        contacts.contact_name,
        contacts.created_on as contact_created_date,
        contacts.last_seen_on as contact_last_seen,
        contacts.fields,
        contacts.is_active as is_contact_active,
        contacts.language
    from msg_urns

    left join contacts on msg_urns.contact_id = contacts.contact_id
)
select * from msgs_full_contacts

