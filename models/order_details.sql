
select 
      od.order_id
    , od.product_id
    , od.unit_price
    , od.quantity
    , pr.product_name
    , pr.supplier_id
    , pr.category_id
    , od.unit_price * od.quantity as total
    ,(pr.unit_price * od.quantity) - total as discount
from {{ source('northwind', 'order_details') }} od
    left join {{ source('northwind', 'products') }} pr on (od.product_id = pr.product_id)