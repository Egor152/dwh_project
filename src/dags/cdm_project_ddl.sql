CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger(
id INT NOT NULL GENERATED ALWAYS AS IDENTITY, -- идентификатор записи. автоинкремент
courier_id INT NOT NULL, -- ID курьера, которому перечисляем. **находится в api курьеров и доставок**
courier_name VARCHAR NOT NULL, -- Ф. И. О. курьера. **находится в api курьеров**
settlement_year INT NOT NULL, -- год отчёт 
settlement_month INT NOT NULL, -- месяц отчёта, где `1` — январь и `12` — декабрь.
orders_count INT NOT NULL DEFAULT 0, -- количество заказов за период (месяц)
orders_total_sum NUMERIC(14,2) NOT NULL DEFAULT 0, -- общая стоимость заказов. стоимость заказа находится в api доставок
rate_avg NUMERIC(4,2) NOT NULL, -- средний рейтинг курьера по оценкам пользователей
order_processing_fee NUMERIC(14,2) NOT NULL DEFAULT 0, --сумма, удержанная компанией за обработку заказов, которая высчитывается как `orders_total_sum * 0.25`.
courier_order_sum NUMERIC(14,2) NOT NULL DEFAULT 0, /* сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. 
						За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга*/
courier_tips_sum NUMERIC(14,2) NOT NULL DEFAULT 0, -- сумма, которую пользователи оставили курьеру в качестве чаевых. чаевые (tips) находятся в api доставок
courier_reward_sum NUMERIC(14,2) NOT NULL DEFAULT 0, -- сумма, которую необходимо перечислить курьеру. Вычисляется как `courier_order_sum + courier_tips_sum * 0.95` (5% — комиссия за обработку платежа).
UNIQUE (courier_id)
);
CREATE INDEX IF NOT EXISTS IDX_dm_courier_ledger__courier_id ON cdm.dm_courier_ledger(courier_id);
CREATE UNIQUE INDEX IF NOT EXISTS IDX_dm_courier_ledger__courier_id ON cdm.dm_courier_ledger(courier_id);