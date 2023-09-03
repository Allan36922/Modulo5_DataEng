--------------------------------------
-- Creacion de tablas externas base de datos Retail_db
--------------------------------------
use retail_db
GO


DROP EXTERNAL TABLE departments
GO


CREATE EXTERNAL TABLE departments (
  department_id int,
  department_name varchar(45)  
)
with (
LOCATION = 'storagebqsilver/departments.parquet',
DATA_SOURCE = RetailCredential2,
FILE_FORMAT = ParquetFileFormat
)
GO


SELECT TOP (10) *
 FROM [dbo].[departments]
 GO
 
-----------------

 DROP EXTERNAL TABLE categories
GO

CREATE EXTERNAL TABLE categories 
(
  category_id int,
  category_department_id int,
  category_name varchar(45)
)
with (
LOCATION = 'storagebqsilver/categories.parquet',
DATA_SOURCE = RetailCredential2,
FILE_FORMAT = ParquetFileFormat
)
GO


SELECT TOP (5) *
FROM [dbo].[categories]
GO

----------------------



DROP EXTERNAL TABLE customers
GO

CREATE EXTERNAL TABLE customers (
  customer_id int,
  customer_fname varchar(45),
  customer_lname varchar(45),
  customer_email varchar(45),
  customer_password varchar(45),
  customer_street varchar(255),
  customer_city varchar(45),
  customer_state varchar(45),
  customer_zipcode varchar(45)
)
with (
LOCATION = 'storagebqsilver/customers.parquet',
DATA_SOURCE = RetailCredential2,
FILE_FORMAT = ParquetFileFormat
)
GO

SELECT TOP (5) *
FROM [dbo].[customers]
GO

---------------------------------
DROP EXTERNAL TABLE order_items
GO

CREATE EXTERNAL TABLE order_items (
  order_item_id int,
  order_item_order_id int,
  order_item_product_id int,
  order_item_quantity tinyint,
  order_item_subtotal float,
  order_item_product_price float
 )
with (
LOCATION = 'storagebqsilver/order_items.parquet',
DATA_SOURCE = RetailCredential2,
FILE_FORMAT = ParquetFileFormat
)
GO 

---------------------------------
DROP EXTERNAL TABLE orders
GO


CREATE EXTERNAL TABLE orders (
  order_id int,
  order_date varchar(10),
  order_customer_id int,
  order_status varchar(45)
)
with (
LOCATION = 'storagebqsilver/orders.parquet',
DATA_SOURCE = RetailCredential2,
FILE_FORMAT = ParquetFileFormat
)
GO 

---------------------------------
DROP EXTERNAL TABLE products
GO

CREATE EXTERNAL TABLE products (
  product_id int,
  product_category_id int,
  product_name varchar(45),
  product_description varchar(255),
  product_price float,
  product_image varchar(255)
)
with (
LOCATION = 'storagebqsilver/products.parquet',
DATA_SOURCE = RetailCredential2,
FILE_FORMAT = ParquetFileFormat
)
GO 

