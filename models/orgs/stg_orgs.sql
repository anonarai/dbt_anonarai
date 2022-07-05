select 
    id as org_id,
    name as org_name,
    created_on
from airbyte_db.ccl.orgs_org
    where name not like '%Demo%'
    and name not like '%Sandbox%'
    and name not like '%App%'
    and name not like '%FB%'
    and name not like '%Simulation%'