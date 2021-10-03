-- 1. вывести количество фильмов в каждой категории, отсортировать по убыванию.
select 
    c.name as category_name
    ,count(distinct film_id) as count
from category as c
left join film_category as fc on c.category_id = fc.category_id
group by c.category_id
order by count desc;


-- 2. вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
select   a.actor_id
        ,a.first_name
        ,a.last_name
        ,count(distinct r.rental_id) as rental_count
from actor a
left join film_actor fa on a.actor_id = fa.actor_id
left join inventory i on i.film_id = fa.film_id
left join rental r on r.inventory_id = i.inventory_id
group by a.actor_id, a.first_name, a.last_name
order by rental_count desc
LIMIT 10;


--3. вывести категорию фильмов, на которую потратили больше всего денег.
select 
    c.name as category_name
    ,sum(p.amount) as category_amount
from category as c
left join film_category as fc on c.category_id = fc.category_id
left join inventory i on i.film_id = fc.film_id
left join rental r on r.inventory_id = i.inventory_id
left join payment p on r.rental_id = p.rental_id
group by c.category_id
order by category_amount desc
limit 1;


-- 4. вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.
select *
from film f
where not exists(select 1 from inventory i where i.film_id = f.film_id)


-- 5. вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
WITH cte AS (
    select a.actor_id, a.first_name, a.last_name, COUNT(fc.film_id) as count, rank() OVER (ORDER BY COUNT(fc.film_id) desc) as rank
    from actor a
    left join film_actor fa on fa.actor_id = a.actor_id
    left join film_category fc on fc.film_id = fa.film_id
    left join category c on fc.category_id = c.category_id
    where c.name = 'Children'
    group by a.actor_id
    order by count desc)
SELECT actor_id, first_name, last_name
FROM   cte
WHERE  rank <= 3;


-- 6. вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.
select 
    c.city
    ,count(case cm.active when 1 then 1 end) as active_count
    ,count(case cm.active when 0 then 1 end) as inactive_count      
from city c
left join address a on c.city_id = a.city_id
left join customer cm on cm.address_id = a.address_id 
group by c.city
order by active_count desc


-- 7. вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), 
-- и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.
select name from (
    select 
        c.name
    from category as c
    left join film_category as fc on c.category_id = fc.category_id
    left join inventory i on i.film_id = fc.film_id
    left join rental r on r.inventory_id = i.inventory_id
    left join customer cu on cu.customer_id = r.customer_id
    left join address a on a.address_id = cu.address_id
    join city ct on ct.city_id = a.city_id
    where ct.city ilike 'a%'
    group by c.category_id
    order by SUM(DATE_PART('hour', r.return_date - r.rental_date)) desc
    limit 1
) as T1
UNION
select name from (
    select 
        c.name
    from category as c
    left join film_category as fc on c.category_id = fc.category_id
    left join inventory i on i.film_id = fc.film_id
    left join rental r on r.inventory_id = i.inventory_id
    left join customer cu on cu.customer_id = r.customer_id
    left join address a on a.address_id = cu.address_id
    join city ct on ct.city_id = a.city_id
    where ct.city ilike '%-%'
    group by c.category_id
    order by SUM(DATE_PART('hour', r.return_date - r.rental_date)) desc
    limit 1
) as T2