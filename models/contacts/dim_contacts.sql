with contacts as (
    select * from {{ ref('stg_contacts' )}}
),

urns as (
    select * from {{ ref('stg_urns' )}}
),

final as (
    select * from contacts
    left join urns using (urns.contact_id, urns.org_id, urns.org_name)
)
select * from final