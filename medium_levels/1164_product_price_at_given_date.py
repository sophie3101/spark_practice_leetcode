""" sql solution
WITH last_change_cte AS (
	SELECT product_id, MAX(change_date) AS last_change_date
	FROM Products
    WHERE change_date <='2019-08-16'
	GROUP BY product_id
	ORDER BY product_id
), 
last_change_price AS (
	SELECT 
		p.product_id , p.new_price as price
	FROM Products p
	JOIN last_change_cte p2
		ON p.product_id = p2.product_id AND p.change_date = p2.last_change_date
	WHERE p2.last_change_date <= '2019-08-16'
		),
product_cte AS (
	SELECT distinct product_id, 10 as original_price
	FROM Products
)

SELECT p2.product_id , COALESCE(p1.price, p2.original_price) as price
FROM last_change_price p1
RIGHT JOIN product_cte p2 
	ON p1.product_id = p2.product_id """
