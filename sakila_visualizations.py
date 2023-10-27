from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt

pss = ""

# Replace 'username' and 'password' with your MySQL username and password
engine = create_engine(f'mysql://root:{pss}@localhost/sakila')

plt.figure(figsize=(12, 6))


###############################################################################################
plt.title('Top Film Revenues')

query = """
SELECT 
    f.film_id,
    f.title AS film_title,
    SUM(p.amount) AS total_revenue
FROM
    film AS f
        JOIN
    inventory AS i ON f.film_id = i.film_id
        JOIN
    rental AS r ON i.inventory_id = r.inventory_id
        JOIN
    payment AS p ON r.rental_id = p.rental_id
GROUP BY f.film_id , film_title
ORDER BY total_revenue DESC
LIMIT 5;
"""

rental_data = pd.read_sql(query, engine)
plt.bar(rental_data['film_title'], rental_data['total_revenue'])

plt.show()


###############################################################################################
plt.title('Top Actors')

query = """
SELECT 
    concat(a.first_name, " ", a.last_name) as actor_name, COUNT(fa.film_id) as films
FROM
    actor AS a
        JOIN
    film_actor AS fa ON a.actor_id = fa.actor_id
GROUP BY a.first_name , a.last_name
HAVING films > 15
ORDER BY films DESC;
"""

rental_data = pd.read_sql(query, engine)
plt.bar(rental_data['actor_name'], rental_data['films'])

plt.show()


###############################################################################################
plt.title('CustomerPayments')

query = """
SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    SUM(p.amount) AS total_payments
FROM
    customer AS c
        JOIN
    payment AS p ON c.customer_id = p.customer_id
GROUP BY c.customer_id , customer_name
ORDER BY total_payments DESC;
"""

rental_data = pd.read_sql(query, engine)
plt.bar(rental_data['customer_id'], rental_data['total_payments'])

plt.show()


###############################################################################################
plt.title('Top Film Revenues')

query = """
SELECT
    category_name,
    film_title,
    rental_count
FROM (
    SELECT
        c.name AS category_name,
        f.title AS film_title,
        COUNT(*) AS rental_count,
        ROW_NUMBER() OVER (PARTITION BY c.name ORDER BY COUNT(*) DESC) AS p
    FROM
        category AS c
    JOIN
        film_category AS fc
        ON c.category_id = fc.category_id
    JOIN
        film AS f
        ON fc.film_id = f.film_id
    JOIN
        inventory AS i
        ON f.film_id = i.film_id
    JOIN
        rental AS r
        ON i.inventory_id = r.inventory_id
    GROUP BY
        c.name, f.title
) AS RankedFilms
WHERE
    p <= 3
ORDER BY
    category_name, p;
"""

rental_data = pd.read_sql(query, engine)
plt.bar(rental_data['film_title'], rental_data['rental_count'])

plt.show()


###############################################################################################
plt.title('Categories over average length')

query = """
WITH CategoryAvgLength AS (
    SELECT
        c.name AS category_name,
        AVG(f.length) AS avg_length
    FROM
        film AS f
    JOIN
        film_category AS fc
        ON f.film_id = fc.film_id
    JOIN
        category AS c
        ON fc.category_id = c.category_id
    GROUP BY
        c.name
),
OverallAvgLength AS (
    SELECT
        AVG(length) AS overall_avg_length
    FROM
        film
)

SELECT
    cat.category_name,
    cat.avg_length
FROM
    CategoryAvgLength AS cat
JOIN
    OverallAvgLength AS overall
WHERE
    cat.avg_length > overall.overall_avg_length;
"""

rental_data = pd.read_sql(query, engine)
plt.bar(rental_data['category_name'], rental_data['avg_length'])

plt.show()