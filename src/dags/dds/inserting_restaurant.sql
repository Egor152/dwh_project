DELETE FROM dds.dm_restaurants;
INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from)
SELECT object_id ,
	   object_value::json->>'name' AS rest_name,
	   update_ts AS update_ts
FROM stg.ordersystem_restaurants AS sor
ORDER BY object_id;