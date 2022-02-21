select
    product,
    count(*)
from
    %s.purchase_events pe
where
        name = 'UPGRADED'
  and revenue_usd_micros > 0
group by
    product