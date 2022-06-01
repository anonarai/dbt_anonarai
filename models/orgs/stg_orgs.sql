select 
    id as org_id,
    name as org_name,
    created_on
from pc_fivetran_db.pg_public.orgs_org
    where name not like '%Demo%'
    and name not like '%Sandbox%'
    and name not like '%App%'
    and name not like '%FB%'
    and name not like '%Simulation%'