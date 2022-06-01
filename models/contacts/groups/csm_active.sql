with contacts as  (
    select * from {{ ref('stg_contacts' )}}
),
csm_groups as(
    select 
        pc_fivetran_db.pg_public.contacts_contactgroup.name as name,
        pc_fivetran_db.pg_public.contacts_contactgroup.id as group_id,
        pc_fivetran_db.pg_public.contacts_contactgroup.org_id as org_id
    from pc_fivetran_db.pg_public.contacts_contactgroup
        where (pc_fivetran_db.pg_public.contacts_contactgroup.name not like '%-Out%' 
        and pc_fivetran_db.pg_public.contacts_contactgroup.name not like '%-out%' 
        and pc_fivetran_db.pg_public.contacts_contactgroup.name not like '%Stopped%')
        and pc_fivetran_db.pg_public.contacts_contactgroup.org_id = 14
),
group_contacts as (

    SELECT 
        pc_fivetran_db.pg_public.contacts_contactgroup_contacts.id as id,
        pc_fivetran_db.pg_public.contacts_contactgroup_contacts.contact_id as contact_id,
        pc_fivetran_db.pg_public.contacts_contactgroup_contacts.contactgroup_id as group_id 
    FROM pc_fivetran_db.pg_public.contacts_contactgroup_contacts
),
groups as (
    select
        group_contacts.contact_id,
        group_contacts.group_id,
        group_contacts.id
    from group_contacts
    inner join csm_groups on csm_groups.group_id = group_contacts.group_id
),
final as (
    select 
        contacts.contact_id,
        contacts.contact_name,
        contacts.created_on,
        contacts.language,
        contacts.last_seen_on,
        groups.group_id,
        groups.id
    from contacts
    inner join groups using (groups.contact_id)
),
msgs as (
    SELECT * 
        FROM pc_fivetran_db.pg_public.msgs_msg
    where pc_fivetran_db.pg_public.msgs_msg.id IN (SELECT MAX(pc_fivetran_db.pg_public.msgs_msg.id) FROM pc_fivetran_db.pg_public.msgs_msg 
    GROUP BY pc_fivetran_db.pg_public.msgs_msg.contact_id)
    and pc_fivetran_db.pg_public.msgs_msg.org_id = 14
    and pc_fivetran_db.pg_public.msgs_msg.direction = 'O'
),
final_2 as (
    select final.id,
        final.contact_id,
        final.created_on,
        final.language,
        final.last_seen_on,
        final.group_id,
        final.contact_name,
        msgs.sent_on
    from final
    inner join msgs on final.contact_id = msgs.contact_id
)
select * from final_2
where final_2.id IN (SELECT MAX(final_2.id) FROM final_2
GROUP BY final_2.contact_id)

