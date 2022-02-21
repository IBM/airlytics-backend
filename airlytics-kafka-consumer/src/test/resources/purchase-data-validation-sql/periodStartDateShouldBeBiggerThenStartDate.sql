select
    product,
    count(*)
from
    %s.purchases pe
where
        period_start_date < start_date
group by
    product