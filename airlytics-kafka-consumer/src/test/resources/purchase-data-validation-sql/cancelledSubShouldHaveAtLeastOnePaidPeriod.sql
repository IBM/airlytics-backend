select
    p.product,
    count(*)
from
    %s.purchase_events p
where
        p."name" = 'CANCELED'
  and p.revenue_usd_micros < 0
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