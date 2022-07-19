with usage as (
    select 
        *
    from PC_FIVETRAN_DB.twilio_gnp.usage_record
),

final as (
    select
        usage.*,
        friendly_name as account_name
    from PC_FIVETRAN_DB.twilio_gnp.account_history
    full outer join usage on usage.account_id = PC_FIVETRAN_DB.twilio_gnp.account_history.id
)

select * from final