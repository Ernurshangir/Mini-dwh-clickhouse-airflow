



select
    1
from `staging`.`dm_sales_daily`

where not(total_orders >= 0)

