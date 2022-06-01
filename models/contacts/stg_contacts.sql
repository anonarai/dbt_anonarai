with orgs as  (
    select * from {{ ref('stg_orgs' )}}
),

contacts as (

    select 
        pc_fivetran_db.pg_public.contacts_contact.id as contact_id,
        pc_fivetran_db.pg_public.contacts_contact.name as contact_name,
        pc_fivetran_db.pg_public.contacts_contact.created_on,
        pc_fivetran_db.pg_public.contacts_contact.last_seen_on,
        year(pc_fivetran_db.pg_public.contacts_contact.modified_on) as year,
        pc_fivetran_db.pg_public.contacts_contact.org_id,
        pc_fivetran_db.pg_public.contacts_contact.language
    from pc_fivetran_db.pg_public.contacts_contact
),

final as (
    select 
        contacts.contact_id,
        contacts.contact_name,
        contacts.created_on,
        contacts.language,
        contacts.year,
        contacts.last_seen_on,
        orgs.org_name,
        orgs.org_id
    from orgs

    inner join contacts on contacts.org_id = orgs.org_id
)

select * from final


