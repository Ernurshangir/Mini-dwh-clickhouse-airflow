

  create or replace view `staging`.`stg_orders` 
  
    
  
  
    
    
  as (
    

SELECT
    order_id,
    customer_id,
    status,
    -- Просто переименовываем колонку
    order_ts AS ordered_at
FROM `raw`.`raw_orders`
ORDER BY order_ts DESC
LIMIT 1 BY order_id
    
  )
      
      
                    -- end_of_sql
                    
                    