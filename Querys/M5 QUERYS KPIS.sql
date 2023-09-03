/*
Proyecto M5 
kpis a partir de las tablas migradas desde MySql
*/


use `retail_db`;

-- •	Ventas totales por mes: 
-- > SQL
-- Este KPI muestra el valor total de las ventas realizadas en cada mes del año.
SELECT 
month(o.order_date) AS mes, 
round(SUM(ot.order_item_subtotal),2) AS total_sales
FROM orders o
JOIN order_items ot ON o.order_id = ot.order_item_order_id
GROUP BY mes
ORDER BY mes;

-- •	Cantidad de productos vendidos por categoría: Este KPI muestra el número de productos vendidos en cada categoría de producto.
-- > SQL

SELECT 
category_name, 
COUNT(order_item_id) AS products_sold
FROM categories JOIN products ON category_id = product_category_id
JOIN order_items ON product_id = order_item_product_id
GROUP BY category_name
ORDER BY products_sold DESC;


-- •	Promedio de precio por producto: Este KPI muestra el precio promedio de cada producto vendido.
-- > SQL

SELECT 
product_name, 
round(AVG(order_item_product_price),2) AS average_price
FROM products JOIN order_items ON product_id = order_item_product_id
GROUP BY product_name
ORDER BY average_price DESC;


-- •	Tasa de conversión por cliente: Este KPI muestra el porcentaje de clientes que realizaron al menos una compra sobre el total de clientes registrados.
-- > SQL
SELECT 
ROUND(COUNT(DISTINCT order_customer_id) / COUNT(DISTINCT customer_id) * 100, 2) AS conversion_rate
FROM customers 
LEFT JOIN orders ON customer_id = order_customer_id;

-- •	Valor promedio del pedido por cliente: 
-- Este KPI muestra el valor promedio de los pedidos realizados por un cliente.
-- > SQL

SELECT 
customer_fname, 
customer_lname, 
AVG(total_order) AS average_order_value
FROM customers 
JOIN (
		SELECT 
        o.order_customer_id, 
        round(SUM(ot.order_item_subtotal),2) AS total_order 
        FROM orders o
        JOIN order_items ot ON o.order_id = ot.order_item_order_id 
        GROUP BY o.order_customer_id
	) AS sub ON customer_id = order_customer_id
GROUP BY customer_fname, customer_lname
ORDER BY average_order_value DESC;

-- •	Tasa de cancelación de pedidos: 
-- Este KPI muestra el porcentaje de pedidos que fueron cancelados sobre el total de pedidos realizados.
-- > SQL

SELECT 
ROUND(COUNT(CASE WHEN order_status = 'CANCELED' THEN 1 END) / COUNT(order_id) * 100, 2) AS cancellation_rate
FROM orders;

-- •	Rentabilidad por producto: Este KPI muestra la diferencia entre el precio de venta y el costo de producción de cada producto vendido.
-- > SQL

SELECT product_name, order_item_product_price - product_cost AS profit
FROM products JOIN order_items ON product_id = order_item_product_id;


-- •	Fidelización del cliente: Este KPI muestra el porcentaje de clientes que realizaron más de una compra sobre el total de clientes que realizaron al menos una compra.
-- > SQL

SELECT 
ROUND(COUNT(CASE WHEN purchases > 1 THEN 1 END) / COUNT(customer_id) * 100, 2) AS retention_rate
FROM customers 
JOIN (
		SELECT order_customer_id, COUNT(DISTINCT order_id) AS purchases 
		FROM orders GROUP BY order_customer_id
	) AS sub ON customer_id = order_customer_id;


