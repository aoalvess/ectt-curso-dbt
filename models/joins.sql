with prod as (
    select
        ct.category_name
        , sp.company_name suppliers
        , pd.product_name
        , pd.unit_price
        , pd.product_id
    from {{source('northwind','products')}} pd
    left join {{source('northwind','suppliers')}} sp on (pd.supplier_id = sp.supplier_id)
    left join {{source('northwind','categories')}} ct on (pd.category_id = ct.category_id)
),
orddetai as (
    select
        pd.*
        , od.order_id
        , od.quantity 
        , od.discount
    from {{ref('order_details')}} od
   left join prod pd on (od.product_id = pd.product_id)
),
ordrs as(
    select
        ord.order_date
        , ord.order_id
        , cs.company_name as customer
        , ep.nome_completo as employee
        , ep.age
        , ep.lengthofservice
    from {{source('northwind','orders')}} ord
        left join {{ref('customers')}} cs on (ord.customer_id = cs.customer_id)
        left join {{ref('employees')}} ep on (ord.employee_id = ep.employee_id)
        left join {{source('northwind','shippers')}} sh on (ord.ship_via = sh.shipper_id)

),
finaljoin as(
    select
        od.*
        , ord.order_date
        , ord.customer
        , ord.employee
        , ord.age
        , ord.lengthofservice
    from orddetai od
    inner join ordrs ord on (od.order_id = ord.order_id)
)
select * from finaljoin