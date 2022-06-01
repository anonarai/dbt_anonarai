select 
    id as channel_id,
    name as channel_name,
    channel_type,
    created_on as channel_created_on,
    schemes,
    org_id

from pc_fivetran_db.pg_public.channels_channel
