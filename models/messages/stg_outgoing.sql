with orgs as(
    select * from {{ ref('stg_orgs')}}
),

channel as  (
    select * from {{ ref('stg_channels' )}}
),

contacts as (

    select 
        pc_fivetran_db.pg_public.msgs_msg.contact_id,
        pc_fivetran_db.pg_public.msgs_msg.channel_id,
        pc_fivetran_db.pg_public.msgs_msg.text,
        pc_fivetran_db.pg_public.msgs_msg.sent_on,
        pc_fivetran_db.pg_public.msgs_msg.org_id,
        pc_fivetran_db.pg_public.msgs_msg.error_count
    from pc_fivetran_db.pg_public.msgs_msg
        where sent_on is not null
        and direction = 'O'
),

all_contacts as (
    select * from contacts

    inner join channel using (channel_id, org_id)
),

final as(
    select * from all_contacts
    inner join orgs using(org_id)
)

select * from final