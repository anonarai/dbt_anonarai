with final as (
	select
		TICKET_ID,
		AGENT,
		CURRENT_STATUS,
		CHANNEL,
		PRIORITY,
		TO_DATE(SPLIT_PART(DATE_CREATED, ' ', 0), 'MM/DD/YYYY') AS created,
		TO_DATE(SPLIT_PART(LAST_UPDATED, ' ', 0), 'MM/DD/YYYY') AS LAST_UPDATED,
		hour(to_time(SPLIT_PART(date_created, '022 ', 2))) AS HOUR,
		CATEGORY,
		customer_name,
		PRIMARY_PHONE_NUMBER,
		description
	from PC_FIVETRAN_DB.DYNAMODB_WEST.WI_TICKETS
	where (created > '05/15/2022' and CHANNEL !='sms')
	or (created> '06/08/2022' and CHANNEL = 'sms')
)

select * from final
where category not like '%Scams%'
and current_status not like 'TEST'
and agent not like 'raj'
and PRIMARY_PHONE_NUMBER not like '%4149990231%'
and PRIMARY_PHONE_NUMBER not like '%4146004800%'
and PRIMARY_PHONE_NUMBER not like '%6126164514%'
and PRIMARY_PHONE_NUMBER not like '%2096045352%'
and PRIMARY_PHONE_NUMBER not like '%5038108069%'