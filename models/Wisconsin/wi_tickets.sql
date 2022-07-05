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
	PRIMARY_PHONE_NUMBER
from PC_FIVETRAN_DB.DYNAMODB_WEST.WI_TICKETS
	where (created > '05/15/2022' and CHANNEL !='sms')
	or (created> '06/08/2022' and CHANNEL = 'sms' and PRIMARY_PHONE_NUMBER !='+15038108069')
	--PRIMARY_PHONE_NUMBER != '6082611455'
	--or PRIMARY_PHONE_NUMBER != '6126164514')