select
	p.product,
	count (*)
from
	%s.purchase_events p
where
	p."name" = 'EXPIRED'
	and (
	select
		count(*)
	from
		%s.purchase_events pe
	where
		pe.purchase_id = p.purchase_id
		and pe."name" = 'TRIAL_STARTED') = 1
	and (
	select
		count(*)
	from
		%s.purchase_events pe
	where
		pe.purchase_id = p.purchase_id
		and pe.revenue_usd_micros > 0) = 0
group by
	p.product