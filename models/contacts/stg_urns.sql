with orgs as  (
    select * from {{ ref('stg_orgs' )}}
),

urn as (

    select 
        airbyte_db.ccl.contacts_contacturn.contact_id,
        airbyte_db.ccl.contacts_contacturn.id as contact_urn,
        airbyte_db.ccl.contacts_contacturn.path as contact,
        airbyte_db.ccl.contacts_contacturn.scheme as channel,
        airbyte_db.ccl.contacts_contacturn.channel_id,
        airbyte_db.ccl.contacts_contacturn.org_id
    from airbyte_db.ccl.contacts_contacturn
),

final as (
    select 
        urn.contact_id,
        urn.contact_urn,
        urn.contact,
        urn.channel,
        urn.channel_id,
        orgs.org_name,
        orgs.org_id
    from orgs

    inner join urn using (urn.org_id)
)

select  * from final