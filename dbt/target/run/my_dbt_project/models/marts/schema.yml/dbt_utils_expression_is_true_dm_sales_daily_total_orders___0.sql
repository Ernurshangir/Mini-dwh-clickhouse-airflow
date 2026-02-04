
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `staging`.`dm_sales_daily`

where not(total_orders >= 0)


  
  
    ) dbt_internal_test