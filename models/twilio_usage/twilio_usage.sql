with usage as (
    select 
        account_id,
        description,
        usage,
        usage_unit,
        count,
        count_unit,
        price,
        price_unit,
        start_date,
        end_date
    from PC_FIVETRAN_DB.twilio_gnp.usage_record
),

final as (
    select
        friendly_name as account_name,
        usage.*
    from PC_FIVETRAN_DB.twilio_gnp.account_history
    
    full outer join usage on usage.account_id = PC_FIVETRAN_DB.twilio_gnp.account_history.id
    where PC_FIVETRAN_DB.twilio_gnp.account_history.status like '%active%'
)

select * from final