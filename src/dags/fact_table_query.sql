DELETE FROM dds.fct_couriers_deliveries
WHERE order_ts::timestamp BETWEEN '{{yesterday_ds}}' AND '{{ds}}';

INSERT INTO dds.fct_couriers_deliveries (order_id, delivery_id, order_ts, delivery_ts, delivery_price, courier_id, courier_name, rate, tip_sum)
SELECT dmo.id AS order_id,
       dmd.id AS delivery_id,
       dmd.order_ts::timestamp AS order_ts,
	   delivery_ts::timestamp AS delivery_ts,
	   delivery_sum AS delivery_price,
	   courier_id AS courier_id,
	   dmc.courier_name AS courier_name,
	   rate AS rate,
	   tip_sum AS tip_sum
FROM dds.dm_deliveries AS dmd
INNER JOIN dds.dm_couriers AS dmc ON dmc.id = dmd.courier_id
INNER JOIN dds.dm_orders AS dmo ON dmd.order_key = dmo.order_key
WHERE dmd.order_ts::timestamp BETWEEN '{{yesterday_ds}}' AND '{{ds}}';