-- ======= Data Preparation =======

-- 1. Create table

-- ======= customer dataset =======
create table customer_dataset (
	customer_id varchar(250) not null,
	customer_unique_id VARCHAR(250) null,
	customer_zip_code_prefix int4 null,
	customer_city VARCHAR(250) null,
	customer_state VARCHAR(250) null
);

-- Menghapus kolom customer_zip_code_prefix yang ada
ALTER TABLE customer_dataset DROP COLUMN customer_zip_code_prefix;

-- Menambahkan kolom customer_zip_code_prefix dengan tipe data baru
ALTER TABLE customer_dataset ADD COLUMN customer_zip_code_prefix VARCHAR(250);

-- ======= geolocation (dirty) =======
create table geolocation_dirty (
	geolocation_zip_code_prefix varchar(250) null,
	geolocation_lat float8 null,
	geolocation_lng float8 null,
	geolocation_city VARCHAR(250) null,
	geolocation_state VARCHAR(250) null
);

-- ======= order items dataset =======
create table order_items_dataset (
	order_id VARCHAR(250) null,
	order_item_id int4 null,
	product_id VARCHAR(250) null,
	seller_id VARCHAR(250) null,
	shipping_limit_date timestamp null,
	price float8 null,
	freight_value float8 null
);

-- ======= order payments dataset =======
create table order_payments_dataset (
	order_id VARCHAR(250) null,
	payment_sequential int4 null,
	payment_type VARCHAR(250) null,
	payment_installments int4 null,
	payment_value float8 null
);

-- ======= order reviews dataset =======
create table order_reviews_dataset (
	review_id VARCHAR(250) null,
	order_id VARCHAR(250) null,
	review_score int4 null,
	review_comment_title VARCHAR(250) null,
	review_comment_message text,
	review_creation_date timestamp null,
	review_answer_timestamp timestamp null
);

-- ======= orders dataset =======
create table orders_dataset (
	order_id VARCHAR(250) not null,
	customer_id VARCHAR(250) null,
	order_status VARCHAR(250) null,
	order_purchase_timestamp timestamp null,
	order_approved_at timestamp null,
	order_delivered_carrier_date timestamp null,
	order_delivered_customer_date timestamp null,
	order_estimated_delivery_date timestamp null
);

-- ======= product dataset =======
create table product_dataset (
	column1 int4 null,
	product_id VARCHAR(250) not null,
	product_category_name VARCHAR(250) null,
	product_name_lenght FLOAT8 null,
	product_description_lenght FLOAT8 null,
	product_photos_qty FLOAT8 null,
	product_weight_g FLOAT8 null,
	product_length_cm FLOAT8 null,
	product_height_cm FLOAT8 null,
	product_width_cm FLOAT8 null
);

-- ======= sellers dataset =======
create table sellers_dataset (
	seller_id VARCHAR(250) not null,
	seller_zip_code_prefix VARCHAR(250) null,
	seller_city VARCHAR(250) null,
	seller_state VARCHAR(250) null
);

-- 2. Import Dataset

copy customer_dataset (
	customer_id,
	customer_unique_id,
	customer_zip_code_prefix,
	customer_city,
	customer_state
)
from
'/Users/kiii_rey/Documents/Kirey/Data science learning/Mini project 1/Dataset/customer_dataset.csv'
delimiter ','
csv header;

copy geolocation_dirty (
	geolocation_zip_code_prefix,
	geolocation_lat,
	geolocation_lng,
	geolocation_city,
	geolocation_state
)
from
'/Users/kiii_rey/Documents/Kirey/Data science learning/Mini project 1/Dataset/geolocation_dirty.csv'
delimiter ','
csv header;

copy order_items_dataset (
	order_id,
	order_item_id,
	product_id,
	seller_id,
	shipping_limit_date,
	price,
	freight_value
)
from
'/Users/kiii_rey/Documents/Kirey/Data science learning/Mini project 1/Dataset/order_items_dataset.csv'
delimiter ','
csv header;

copy order_payments_dataset (
	order_id,
	payment_sequential,
	payment_type,
	payment_installments,
	payment_value
)
from
'/Users/kiii_rey/Documents/Kirey/Data science learning/Mini project 1/Dataset/order_payments_dataset.csv'
delimiter ','
csv header;

copy order_reviews_dataset (
	review_id,
	order_id,
	review_score,
	review_comment_title,
	review_comment_message,
	review_creation_date,
	review_answer_timestamp,
	PRIMARY KEY (order_id)
)
from
'/Users/kiii_rey/Documents/Kirey/Data science learning/Mini project 1/Dataset/order_reviews_dataset.csv'
delimiter ','
csv header;

copy orders_dataset (
	order_id,
	customer_id,
	order_status,
	order_purchase_timestamp,
	order_approved_at,
	order_delivered_carrier_date,
	order_delivered_customer_date,
	order_estimated_delivery_date
)
from
'/Users/kiii_rey/Documents/Kirey/Data science learning/Mini project 1/Dataset/orders_dataset.csv'
delimiter ','
csv header;

copy product_dataset (
	column1,
	product_id,
	product_category_name,
	product_name_lenght,
	product_description_lenght,
	product_photos_qty,
	product_weight_g,
	product_length_cm,
	product_height_cm,
	product_width_cm
)
from
'/Users/kiii_rey/Documents/Kirey/Data science learning/Mini project 1/Dataset/product_dataset.csv'
delimiter ','
csv header;

copy sellers_dataset (
	seller_id,
	seller_zip_code_prefix,
	seller_city,
	seller_state
)
from
'/Users/kiii_rey/Documents/Kirey/Data science learning/Mini project 1/Dataset/sellers_dataset.csv'
delimiter ','
csv header;

-- Membuat Tabel Geolocation yang sudah di cleaning
CREATE TABLE geolocation_dirty2 AS
SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
REPLACE(REPLACE(REPLACE(
TRANSLATE(TRANSLATE(TRANSLATE(TRANSLATE(
TRANSLATE(TRANSLATE(TRANSLATE(TRANSLATE(
	geolocation_city, ' , , ,.', ''), '`', ''''),
	' , ', 'e,e'), ' , , ', 'a,a,a'), ' , , ', 'o,o,o'),
		' ', 'c'), ' , ', 'u,u'), ' ', 'i'),
		'4o', '4 '), '* ', ''), '%26apos%3b', ''''
) AS geolocation_city, geolocation_state
from geolocation_dirty gd;

CREATE TABLE geolocation AS
WITH geolocation AS (
	SELECT geolocation_zip_code_prefix,
	geolocation_lat, 
	geolocation_lng, 
	geolocation_city, 
	geolocation_state FROM (
		SELECT *,
			ROW_NUMBER() OVER (
				PARTITION BY geolocation_zip_code_prefix
			) AS ROW_NUMBER
		FROM geolocation_dirty2 
	) TEMP
	WHERE ROW_NUMBER = 1
),
custgeo AS (
	SELECT customer_zip_code_prefix, geolocation_lat, 
	geolocation_lng, customer_city, customer_state 
	FROM (
		SELECT *,
			ROW_NUMBER() OVER (
				PARTITION BY customer_zip_code_prefix
			) AS ROW_NUMBER
		FROM (
			SELECT customer_zip_code_prefix, geolocation_lat, 
			geolocation_lng, customer_city, customer_state
			FROM customer_dataset cd
			LEFT JOIN geolocation_dirty gdd 
			ON customer_city = geolocation_city
			AND customer_state = geolocation_state
			WHERE customer_zip_code_prefix NOT IN (
				SELECT geolocation_zip_code_prefix
				FROM geolocation gd 
			)
		) geo
	) TEMP
	WHERE ROW_NUMBER = 1
),
sellgeo AS (
	SELECT seller_zip_code_prefix, geolocation_lat, 
	geolocation_lng, seller_city, seller_state 
	FROM (
		SELECT *,
			ROW_NUMBER() OVER (
				PARTITION BY seller_zip_code_prefix
			) AS ROW_NUMBER
		FROM (
			SELECT seller_zip_code_prefix, geolocation_lat, 
			geolocation_lng, seller_city, seller_state
			FROM sellers_dataset cd 
			LEFT JOIN geolocation_dirty gdd 
			ON seller_city = geolocation_city
			AND seller_state = geolocation_state
			WHERE seller_zip_code_prefix NOT IN (
				SELECT geolocation_zip_code_prefix
				FROM geolocation gd 
				UNION
				SELECT customer_zip_code_prefix
				FROM custgeo cd 
			)
		) geo
	) TEMP
	WHERE ROW_NUMBER = 1
)
SELECT * 
FROM geolocation
UNION
SELECT * 
FROM custgeo
UNION
SELECT * 
FROM sellgeo;

-- 3. Constraint & Foreign Key

-- Add Primary Key
alter table product_dataset add CONSTRAINT products_pk PRIMARY KEY (product_id);
alter table orders_dataset add CONSTRAINT orders_pk PRIMARY KEY (order_id);
alter table customer_dataset add CONSTRAINT customers_pk PRIMARY KEY (customer_id);
alter table sellers_dataset add CONSTRAINT sellers_pk PRIMARY KEY (seller_id);
alter table geolocation add CONSTRAINT geolocation_pk PRIMARY KEY (geolocation_zip_code_prefix);

--  products -> order_items

ALTER TABLE order_items_dataset 
ADD CONSTRAINT order_items_fk_product 
FOREIGN KEY (product_id) REFERENCES product_dataset(product_id) 
ON DELETE CASCADE ON UPDATE CASCADE;

-- sellers -> order_items

ALTER TABLE order_items_dataset
ADD CONSTRAINT order_items_fk_seller 
FOREIGN KEY (seller_id) REFERENCES sellers_dataset(seller_id) 
ON DELETE CASCADE ON UPDATE CASCADE;

-- orders -> order_items

ALTER TABLE order_items_dataset 
ADD CONSTRAINT order_items_fk_order 
FOREIGN KEY (order_id) REFERENCES orders_dataset(order_id) 
ON DELETE CASCADE ON UPDATE CASCADE;

--  orders -> order_payments

ALTER TABLE order_payments_dataset
ADD CONSTRAINT order_payments_fk 
FOREIGN KEY (order_id) REFERENCES orders_dataset(order_id) 
ON DELETE CASCADE ON UPDATE CASCADE;

-- orders -> order_reviews

ALTER TABLE order_reviews_dataset 
ADD CONSTRAINT order_reviews_fk 
FOREIGN KEY (order_id) REFERENCES orders_dataset(order_id) 
ON DELETE CASCADE ON UPDATE CASCADE;

-- customers -> orders

ALTER TABLE orders_dataset
ADD CONSTRAINT orders_fk 
FOREIGN KEY (customer_id) REFERENCES customer_dataset(customer_id) 
ON DELETE CASCADE ON UPDATE CASCADE;

-- geolocation -> customers

ALTER TABLE customer_dataset
ADD CONSTRAINT customers_fk 
FOREIGN KEY (customer_zip_code_prefix) REFERENCES geolocation(geolocation_zip_code_prefix) 
ON DELETE CASCADE ON UPDATE CASCADE;

-- geolocation -> sellers

ALTER TABLE sellers_dataset
ADD CONSTRAINT sellers_fk 
FOREIGN KEY (seller_zip_code_prefix) REFERENCES geolocation(geolocation_zip_code_prefix) 
ON DELETE CASCADE ON UPDATE CASCADE;

-- ======= Annual Customer Activity Growth Analysis =======

-- 1. Menampilkan rata-rata jumlah customer aktif bulanan (monthly active user) untuk setiap tahun

SELECT 
	year,
	floor(avg(n_customers)) AS avg_monthly_active_user
FROM (
	SELECT 
		date_part('year',order_purchase_timestamp) AS year,
		date_part('month',order_purchase_timestamp) AS month,
		count(DISTINCT customer_unique_id) AS n_customers
	FROM orders_dataset o
	JOIN customer_dataset c
	ON o.customer_id = c.customer_id
	GROUP BY 1,2
) monthly
GROUP BY 1
ORDER BY 1;

-- 2. Menampilkan jumlah customer baru pada masing-masing tahun

SELECT
	date_part('year', first_date_order) AS year,
	count(customer_unique_id) AS new_customers
FROM (
	SELECT 
		c.customer_unique_id,
		min(order_purchase_timestamp) AS first_date_order
	FROM orders_dataset o
	JOIN customer_dataset c
	ON o.customer_id = c.customer_id
	GROUP BY 1
) first_order
GROUP BY 1
ORDER BY 1;

-- 3. Menampilkan jumlah customer yang melakukan pembelian lebih dari satu kali (repeat order) pada masing-masing tahun

SELECT 
	year,
	count(DISTINCT customer_unique_id) AS customers_repeat
FROM (
	SELECT 
		date_part('year',o.order_purchase_timestamp)AS year,
		c.customer_unique_id,
		count(c.customer_unique_id)AS n_customer,
		count(o.order_id) AS n_order
	FROM orders_dataset o
	JOIN customer_dataset c
	ON o.customer_id = c.customer_id
	GROUP BY 1,2
	HAVING count(o.order_id) > 1
) order_repeat
GROUP BY 1
ORDER BY 1;

-- 4. Menampilkan rata-rata jumlah order yang dilakukan customer untuk masing-masing tahun

SELECT 
	year,
	round(avg(n_order), 2) AS avg_num_orders
FROM (
	SELECT 
		date_part('year',o.order_purchase_timestamp)AS year,
		c.customer_unique_id,
		count(c.customer_unique_id)AS n_customer,
		count(o.order_id) AS n_order
	FROM orders_dataset o
	JOIN customer_dataset c
	ON o.customer_id = c.customer_id
	GROUP BY 1,2
) order_customer
GROUP BY 1
ORDER BY 1;

-- 5. Menggabungkan ketiga metrik yang telah berhasil ditampilkan menjadi satu tampilan tabel

WITH table_mau AS (
	SELECT
		year,
		floor(avg(n_customers)) AS avg_monthly_active_user
	FROM (
		SELECT
			date_part('year',order_purchase_timestamp) AS year,
			date_part('month',order_purchase_timestamp) AS month,
			count(DISTINCT customer_unique_id) AS n_customers
		FROM orders_dataset o
		JOIN customer_dataset c
		ON o.customer_id = c.customer_id
		GROUP BY 1,2
	) monthly
	GROUP BY 1
	ORDER BY 1
),
table_newcust AS (
	SELECT 
		date_part('year', first_date_order) AS year,
		count(customer_unique_id) AS new_customers
	FROM (
		SELECT 
			c.customer_unique_id,
			min(order_purchase_timestamp) AS first_date_order
		FROM orders_dataset o
		JOIN customer_dataset c
		ON o.customer_id = c.customer_id
		GROUP BY 1
	) first_order
GROUP BY 1
ORDER BY 1
),
table_cust_repeat AS (
	SELECT 
		year,
		count(DISTINCT customer_unique_id) AS customers_repeat
	FROM (
		SELECT 
			date_part('year',o.order_purchase_timestamp)AS year,
			c.customer_unique_id,
			count(c.customer_unique_id)AS n_customer,
			count(o.order_id) AS n_order
		FROM orders_dataset o
		JOIN customer_dataset c
		ON o.customer_id = c.customer_id
		GROUP BY 1,2
		HAVING count(o.order_id) > 1
	) order_repeat
	GROUP BY 1
	ORDER BY 1
),
table_avg_order AS (
	SELECT 
		year,
			round(avg(n_order), 2) AS avg_num_orders
	FROM (
		SELECT 
			date_part('year',o.order_purchase_timestamp)AS year,
			c.customer_unique_id,
			count(c.customer_unique_id)AS n_customer,
			count(o.order_id) AS n_order
		FROM orders_dataset o
		JOIN customer_dataset c
		ON o.customer_id = c.customer_id
		GROUP BY 1,2
	) order_customer
	GROUP BY 1
	ORDER BY 1
)
SELECT 
	tm.year,
	avg_monthly_active_user,
	new_customers,
	customers_repeat,
	avg_num_orders
FROM table_mau tm
JOIN table_newcust tn
ON tm.year = tn.year
JOIN table_cust_repeat tr
ON tm.year = tr.year
JOIN table_avg_order ta
ON tm.year = ta.year
ORDER BY 1;

-- ======= Annual Product Category Quality Analysis =======

-- 1. Tabel yang berisi informasi pendapatan/revenue perusahaan total untuk masing-masing tahun

CREATE TABLE total_revenue_year AS
WITH revenue_orders AS (
	SELECT 
		order_id,
		sum(price + freight_value) AS revenue
	FROM order_items_dataset oi
	GROUP BY 1
	)
	SELECT
		date_part('year',o.order_purchase_timestamp) AS year,
		sum(ro.revenue)AS revenue
	FROM orders_dataset o
	JOIN revenue_orders ro
	ON o.order_id = ro.order_id
	WHERE o.order_status = 'delivered'
	GROUP BY 1
	ORDER BY 1;

-- 2. Tabel yang berisi informasi jumlah cancel order total untuk masing-masing tahun

CREATE TABLE total_canceled_orders_year AS
SELECT 
	date_part('year', order_purchase_timestamp) AS year,
	count(order_id) AS total_canceled
FROM orders_dataset o
WHERE order_status ='canceled'
GROUP BY 1
ORDER BY 1;

-- 3. tabel yang berisi nama kategori produk yang memberikan pendapatan total tertinggi untuk masing-masing tahun

CREATE TABLE top_product_category_revenue_year AS
WITH revenue_category_orders AS (
			SELECT
				date_part('year',o.order_purchase_timestamp) AS year,
				p.product_category_name,
				sum(price + freight_value) AS revenue,
				ROW_NUMBER() OVER(
				      PARTITION BY date_part('year', o.order_purchase_timestamp)
				      ORDER BY sum(price + freight_value)desc
				) AS rank
		FROM orders_dataset o
		JOIN order_items_dataset oi
		ON o.order_id = oi.order_id
		JOIN product_dataset p
		ON oi.product_id = p.product_id
		WHERE order_status = 'delivered'
		GROUP BY 1,2
		)
		SELECT 
			year,
			product_category_name,
			revenue
		FROM revenue_category_orders
		WHERE rank = 1;
		
-- 4. Tabel yang berisi nama kategori produk yang memiliki jumlah cancel order terbanyak untuk masing-masing tahun
CREATE TABLE top_product_category_canceled_year AS
WITH canceled_category_orders AS (
		SELECT
			date_part('year',o.order_purchase_timestamp) AS year,
			p.product_category_name,
			count(*) AS total_canceled,
			ROW_NUMBER() OVER(
			   	PARTITION BY date_part('year', o.order_purchase_timestamp)
				ORDER BY count(*) desc
		) AS rank
FROM orders_dataset o
JOIN order_items_dataset oi
ON o.order_id = oi.order_id
JOIN product_dataset p
ON oi.product_id = p.product_id
WHERE order_status = 'canceled'
GROUP BY 1,2
)
SELECT 
	year,
	product_category_name,
	total_canceled
FROM canceled_category_orders
WHERE rank = 1;

-- 5. Menggabungkan informasi-informasi yang telah didapatkan ke dalam satu tampilan  tabel

SELECT 
		tpr.year,
		tpr.product_category_name AS top_product_category_revenue,
		tpr.revenue AS top_category_revenue,
		try.revenue AS total_revenue_year,
		tpc.product_category_name AS top_product_category_canceled,
		tpc.total_canceled AS top_category_canceled,			
		tco.total_canceled AS total_canceled_orders_year
FROM top_product_category_revenue_year tpr
JOIN total_revenue_year try
ON tpr.year = try.year
JOIN top_product_category_canceled_year tpc
ON tpr.year = tpc.year
JOIN total_canceled_orders_year tco
ON tpr.year = tco.year;

-- ======= Analysis of Annual Payment Type Usage =======

-- 1. informasi jenis tipe pembayaran yang digunakan dalam pesanan beserta jumlah penggunaan (jumlah pesanan) untuk masing-masing jenis pembayaran

SELECT 
	op.payment_type,
	count(*) AS num_of_usage
FROM orders_dataset o
JOIN order_payments_dataset op
ON o.order_id = op.order_id
GROUP BY 1;

-- 2. informasi daftar tahun, jenis tipe pembayaran, dan jumlah penggunaan tiap jenis tipe pembayaran dalam setiap tahun, yang diurutkan berdasarkan tahun secara naik dan jumlah penggunaan secara menurun

SELECT 
	date_part('year', o.order_purchase_timestamp) AS year,
	op.payment_type,
	count(*) AS num_of_usage
FROM orders_dataset o
JOIN order_payments_dataset op
ON o.order_id = op.order_id
GROUP BY 1,2
ORDER BY 1 ASC, 3 DESC;

-- 3. Menampilkan tabel yang berisi informasi jenis tipe pembayaran beserta jumlah pesanan pada tahun 2016, 2017, dan 2018, yang diurutkan berdasarkan jumlah pesanan pada tahun 2018 secara menurun (diurutkan dari yang terfavorit).

SELECT
	payment_type,
	COUNT(CASE WHEN date_part('year', order_purchase_timestamp) = '2016' 
    THEN o.order_id END) AS year_2016, 
	COUNT(CASE WHEN date_part('year', order_purchase_timestamp) = '2017'  
    THEN o.order_id END) AS year_2017,
	COUNT(CASE WHEN date_part('year', order_purchase_timestamp) = '2018' 
 	THEN o.order_id END) AS year_2018
FROM orders_dataset o
JOIN order_payments_dataset op
ON o.order_id = op.order_id
GROUP BY 1
ORDER BY 4 DESC;



