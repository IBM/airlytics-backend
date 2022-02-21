select p.product, count(*) from %s.purchases p where start_date::date > '2021-03-01' and store_response is null and
(select sum(revenue_usd_micros) from %s.purchase_events pe where pe.purchase_id = p.id)!= p.revenue_usd_micros and
(select count(*) from %s.purchase_events pe where pe.purchase_id = p.id
and (pe.name = 'TRIAL_STARTED' or pe.name = 'PURCHASED')) = 1
group by p.product

