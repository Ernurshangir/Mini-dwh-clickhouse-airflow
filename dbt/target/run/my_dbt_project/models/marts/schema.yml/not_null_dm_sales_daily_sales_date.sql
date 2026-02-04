
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sales_date
from `staging`.`dm_sales_daily`
where sales_date is null



  
  
    ) dbt_internal_test