with orgs as  (
    select * from {{ ref('stg_orgs' )}}
),

urn as (

    select 
        pc_fivetran_db.pg_public.contacts_contacturn.contact_id,
        pc_fivetran_db.pg_public.contacts_contacturn.path as contact,
        pc_fivetran_db.pg_public.contacts_contacturn.scheme as channel,
        pc_fivetran_db.pg_public.contacts_contacturn.channel_id,
        pc_fivetran_db.pg_public.contacts_contacturn.org_id
    from pc_fivetran_db.pg_public.contacts_contacturn
),

final as (
    select 
        urn.contact_id,
        urn.contact,
        urn.channel,
        urn.channel_id,
        orgs.org_name,
        orgs.org_id
    from orgs

    inner join urn using (urn.org_id)
)

select * from final
