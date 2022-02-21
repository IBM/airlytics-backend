select
    product,
    count(*)
from
    %s.purchases
where
        active = true
  and expiration_date < now() - interval '2 day'
  and grace = false
group by
    product