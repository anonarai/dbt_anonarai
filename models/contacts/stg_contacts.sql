with orgs as  (
    select * from {{ ref('stg_orgs' )}}
),

contacts as (

    select 
        airbyte_db.ccl.contacts_contact.id as contact_id,
        airbyte_db.ccl.contacts_contact.name as contact_name,
        airbyte_db.ccl.contacts_contact.created_on,
        airbyte_db.ccl.contacts_contact.last_seen_on,
        airbyte_db.ccl.contacts_contact.org_id,
        airbyte_db.ccl.contacts_contact.fields,
        airbyte_db.ccl.contacts_contact.is_active,
        airbyte_db.ccl.contacts_contact.language
    from airbyte_db.ccl.contacts_contact
),

final as (
    select 
        contacts.contact_id,
        contacts.contact_name,
        contacts.created_on,
        contacts.language,
        contacts.fields,
        contacts.is_active,
        contacts.last_seen_on,
        orgs.org_name,
        orgs.org_id
    from orgs

    inner join contacts on contacts.org_id = orgs.org_id
)

select * from final



