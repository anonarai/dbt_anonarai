with contacts as  (
    select * from {{ ref('stg_contacts' )}}
),
group_contacts as (

    select 
        pc_fivetran_db.pg_public.contacts_contactgroup_contacts.contact_id as contact_id,
        pc_fivetran_db.pg_public.contacts_contactgroup_contacts.contactgroup_id as group_id
    from pc_fivetran_db.pg_public.contacts_contactgroup_contacts
        where contactgroup_id = 1122
),
final as (
    select 
        contacts.contact_id,
        contacts.contact_name,
        contacts.created_on,
        contacts.language,
        contacts.last_seen_on,
        group_contacts.group_id
    from contacts
    inner join group_contacts on group_contacts.contact_id = contacts.contact_id
)

select * from final
