
  
    
    
    
        
         


        insert into `staging`.`dm_sales_daily__dbt_backup`
        ("sales_date", "total_orders", "unique_customers", "paid_orders")

SELECT
    toStartOfDay(ordered_at) AS sales_date,
    count(order_id) AS total_orders,
    uniqExact(customer_id) AS unique_customers,
    countIf(status = 'paid') AS paid_orders
FROM `staging`.`stg_orders`
GROUP BY sales_date
  