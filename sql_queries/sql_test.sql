-- How many total messages are being sent every day?
SELECT CAST(createdat AS DATE) created_date,
	   COUNT(*) messages_count
FROM messages
GROUP BY CAST(createdat AS DATE)
ORDER BY 1;


-- Are there any users that did not receive any message?
SELECT u.user_id user_ids_without_msgs_received
FROM users u
	LEFT JOIN messages m ON u.user_id = m.receiverid
WHERE m.receiverid IS NULL;


-- How many active subscriptions do we have today?
SELECT COUNT(*) AS active_subscriptions
	FROM subscriptions s
WHERE status  = 'Active';


--Are there users sending messages without an active subscription?
SELECT DISTINCT  m.senderid users_sending_msgs_without_sub
	FROM messages m
		LEFT JOIN subscriptions s ON m.senderid=s.user_id
WHERE s.user_id IS NULL
	OR (s.user_id IS NOT NULL
	AND m.createdat NOT BETWEEN s.startdate AND s.enddate);


--Did you identified any inaccurate/noisy record that somehow could prejudice the data analyses?
--How to monitor it (SQL query)? Please explain how do you suggest to handle with this noisy data?

SELECT EXTRACT(YEAR FROM CURRENT_DATE) - birth_year age, count(*) cnt
FROM users
GROUP BY EXTRACT(YEAR FROM CURRENT_DATE) - birth_year;