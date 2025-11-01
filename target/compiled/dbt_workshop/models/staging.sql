with customers as (
    select * from main."seed_customers"
),
orders as (
    select * from main."seed_orders"
)
select
    c.customer_id,
    c.first_name,
    c.last_name,
    count(o.order_id) as num_orders,
    sum(o.amount) as total_amount
from customers c
left join orders o on o.customer_id = c.customer_id
group by 1,2,3