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

categories as (
    select 
    usage.*,
    analytics.dbt_anonarai.twilio_categories.description as descrip 
    from analytics.dbt_anonarai.twilio_categories
    full outer join usage on usage.description = analytics.dbt_anonarai.twilio_categories.description
),

final as (
    select 
        PC_FIVETRAN_DB.twilio_gnp.account_history.friendly_name as account_name,
        categories.account_id,
        categories.description,
        categories.usage,
        categories.usage_unit,
        categories.count,
        categories.count_unit,
        categories.price,
        categories.price_unit,
        categories.start_date,
        categories.end_date
    from categories
    full outer join PC_FIVETRAN_DB.twilio_gnp.account_history on PC_FIVETRAN_DB.twilio_gnp.account_history.id =categories.account_id
    where PC_FIVETRAN_DB.twilio_gnp.account_history.status like '%active%'
    and descrip is not null
)
select * from final

