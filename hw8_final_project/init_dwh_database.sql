-- DROP DATABASE DSHOP_DWH;

CREATE DATABASE DSHOP_DWH;

create table if not exists clients_dim (
    client_id int, 
    fullname varchar(30), 
    location_area_id int
) with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (client_id, location_area_id);

create table if not exists products_dim (
    product_id integer,
    product_name varchar(130),
    aisle_id integer,
    department_id integer
) with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (product_id, aisle_id, department_id);

create table if not exists aisles_dim (
    aisle_id integer,
    aisle varchar(30)
) with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (aisle_id);

create table if not exists departments_dim (
    department_id integer,
    department varchar(30)
) with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (department_id);

create table if not exists dates_dim (
    time_id bigint, 
    action_date date,
    action_week int,
    action_month int,
    action_year int,
    action_weekday int
) with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (time_id);

create table if not exists location_areas_dim (
    location_area_id integer
) with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (location_area_id);

create table if not exists orders_fact (
    order_id integer,
    product_id integer,
    client_id integer,
    store_id integer,
    quantity integer,
    time_id bigint
) with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (order_id, product_id, client_id, store_id, time_id);

create table if not exists out_of_stock_fact (
    time_id bigint, 
    product_id integer
) with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (product_id, time_id);

