create database if not exists sparkos_demo;

drop table if exists sparkos_demo.transactions;
create table sparkos_demo.transactions (
  transaction_id string,
  user_id string,
  device_id string,
  amount double
)
partitioned by (dt string)
stored as parquet;

insert into sparkos_demo.transactions partition (dt='2026-06-21')
values
  ('t1', 'u1', 'd1', 120.5),
  ('t2', 'u2', 'd1', 42.3);

insert into sparkos_demo.transactions partition (dt='2026-06-22')
values
  ('t3', 'u1', 'd2', 88.0),
  ('t4', 'u3', 'd3', 300.0);

drop table if exists sparkos_demo.users;
create table sparkos_demo.users (
  user_id string,
  city string,
  status string,
  created_at string
)
stored as parquet;

insert into sparkos_demo.users
values
  ('u1', 'Shanghai', 'active', '2026-01-01'),
  ('u2', 'Beijing', 'active', '2026-02-01'),
  ('u3', 'Shenzhen', 'risk', '2026-03-01');
