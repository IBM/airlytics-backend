select
    product,
    count(*)
from
    %s.purchase_events pe
where
        cancellation_reason = 'REPLACED_WITH_SUB'
  and name = 'EXPIRED'
group by
    product