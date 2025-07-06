--SET hive.metastore.warehouse.dir;

-- создаем бд
create database otus;

-- переключаемся на нее
use otus;

--создаем внешнюю таблицу
drop table if exists airports;
create external table airports (
airport_id int,
city       string,
state      string,
name       string
)
row format delimited
fields terminated by ','
lines  terminated by '\n'
location '/Users/azaliaaknazarova/Downloads/airports';

select * from airports;

-- построение с конструкциями

-- 1. Количество аэропортов по штатам
-- ddl
drop table if exists airports_by_state;
create table if not exists airports_by_state (
 state string,
 airoport_count int
) stored as orc
location '/Users/azaliaaknazarova/hive/warehouse/airports_by_state'; --todo можно убрать и тогда файл будет в стандартной user

-- вставка данных
-- Таблица содержит информацию о штатах, в которых больше 3-х аэропортов (в порядке убывания)
insert into airports_by_state
select
        state
      , count(*) as airport_count
from airports
where state is not null
group by state
having count(*) > 3
order by airport_count desc;

select * from airports_by_state;
select count(*) from airports_by_state; --40
select distinct state from airports; --56

-- 2. аэропорты калифорнии и техаса
drop table if exists ca_tx_airports;

create table if not exists ca_tx_airports (
  airport_id INT,
  city STRING,
  state STRING,
  name STRING
) stored as orc
location '/Users/azaliaaknazarova/hive/warehouse/ca_tx_airports'; --todo можно убрать и тогда файл будет в стандартной user

insert OVERWRITE table ca_tx_airports
select airport_id, city, state, name from airports WHERE state = 'CA'
UNION
select airport_id, city, state, name from airports WHERE state = 'TX';

select * from ca_tx_airports;

-- 3. Ранжирование аэропортов по имени в разрезе штата

drop table if exists ranked_airports;
create table if not exists ranked_airports (
  airport_id INT,
  name STRING,
  state STRING,
  city STRING,
  name_rank INT
) stored as orc
location '/Users/azaliaaknazarova/hive/warehouse/ranked_airports'; --todo можно убрать и тогда файл будет в стандартной user

insert OVERWRITE table ranked_airports
select
  airport_id,
  name,
  state,
  city,
  RANK() OVER (PARTITION BY state ORDER BY name ASC) AS name_rank
from airports;

select * from ranked_airports;

-- 4. Первый по алфавиту аэропорт в каждом штате

drop table if exists top_airport_by_state;
create table if not exists top_airport_by_state (
  airport_id INT,
  name STRING,
  state STRING,
  city STRING
) stored as orc
location '/Users/azaliaaknazarova/hive/warehouse/top_airport_by_state'; --todo можно убрать и тогда файл будет в стандартной user

insert into  table top_airport_by_state
select airport_id, name, state, city
from (
  select
    airport_id,
    name,
    state,
    city,
    ROW_NUMBER() OVER (PARTITION BY state ORDER BY name ASC) AS rn
  from airports
) t
WHERE rn = 1;

select * from top_airport_by_state;

-- 5. оставляем из таблицы со списком первого по алфавиту аэропорта только те штаты,  в которых меньше 3 аэропортов на штат

drop table if exists top_airport_by_state_less_3;
create table if not exists top_airport_by_state_less_3 (
  airport_id INT,
  name STRING,
  state STRING,
  city string
) stored as orc
location '/Users/azaliaaknazarova/hive/warehouse/top_airport_by_state_less_3'; --todo можно убрать и тогда файл будет в стандартной user

SET hive.auto.convert.join=false;

insert into  table top_airport_by_state_less_3
select
t.airport_id, t.name, t.state, t.city
from top_airport_by_state t
left join airports_by_state a
on a.state = t.state
where a.state is null;

select * from top_airport_by_state_less_3;

