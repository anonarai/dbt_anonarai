select
    id as flow_id,
    org_id,
    name as flow_name,
    is_active
from pc_fivetran_db.pg_public.flows_flow