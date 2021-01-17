# TiGraph

TiGraph 项目实现了在 TiDB 无缝集成图模式：

- 在 SQL 中扩展出一个让 DBA 一眼就能学会的图遍历语法
- 同一个事务中操作图数据和关系型数据的能力

[TiGraph 介绍 PPT](https://docs.google.com/presentation/d/1RBe8HUtGqzlyruEyCbzKh8dxOk6D7Zd5dSz1mPXvPoc/edit#slide=id.p)

# Usage

Start the `tidb-server` first.

```shell
git clone https://github.com/tigraph/tidb.git
cd tidb
# build tidb-server binary
make
# start tidb-server
bin/tidb-server
```

## Sample Demo

```sql
drop database if exists example;
create database example;
use example;
-- 创建一个节点定义
CREATE TAG people (id BIGINT, name VARCHAR(32), register TIMESTAMP DEFAULT now(), UNIQUE INDEX (name), index (register));
-- 写入 4 个点
INSERT INTO people VALUES (1, 'Bob', '2010-01-16 18:00:00'),(2,'Max', '2011-01-15 18:00:00'),(3,'Jon', '2012-01-14 18:00:00'),(4,'Jim', '2020-01-13 18:00:00');
-- 创建 follow 边
CREATE EDGE follow (src BIGINT, dst BIGINT, time DATE);
-- 写入 3 条边
INSERT INTO follow VALUES (1, 2, '2010-01-01'), (1, 3, '2012-02-02'), (3, 4, '2020-12-12');
-- 查询 Bob 关注的人（Bob 的 1 度人脉）
SELECT * FROM people WHERE name = 'Bob' TRAVERSE OUT(follow).TAG(people);
-- 查询 Bob 在 2012 年关注的人
SELECT * FROM people WHERE name = 'Bob' TRAVERSE OUT(follow WHERE YEAR(time) = 2012).TAG(people);
-- 查询 Bob 的 2 度人脉
SELECT * FROM people WHERE name = 'Bob' TRAVERSE OUT(follow).OUT(follow).TAG(people);
-- 查询 Bob 的 3 度人脉
SELECT * FROM people WHERE name = 'Bob' TRAVERSE OUT(follow).OUT(follow).OUT(follow).TAG(people);
-- 图遍历查询用 unique index 走 point-get 优化
DESC SELECT * FROM people WHERE name = 'Bob' TRAVERSE OUT(follow).TAG(people);
-- 图遍历查询用 二级索引 走 IndexLookUp 避免搜索全部的图空间，并将其他条件下推到 cop 执行  
DESC SELECT * FROM people use index (register) WHERE register='2010-01-16 18:00:00' and name like 'J%' TRAVERSE OUT(follow).TAG(people);
```

# Benchmark

## 用 TiGraph 进行 N 度人脉测试

1. 准备图节点测试数据

```shell
cd cmd/tigraph-generator/

# 生成 10万 个图节点数据到 data.sql 文件
go run main.go
# 导入数据
mysql -u root -h 127.0.0.1 -P 4000 test < data.sql
```

2. N 度人脉测试 SQL

```sql
use test;
-- 2 度人脉查询
SELECT count(*) FROM people WHERE id=1234 TRAVERSE OUT(friends).OUT(friends).TAG(people);
-- 3 度人脉查询
SELECT count(*) FROM people WHERE id=1234 TRAVERSE OUT(friends).OUT(friends).OUT(friends).TAG(people); 
-- 4 度人脉查询
SELECT count(*) FROM people WHERE id=1234 TRAVERSE OUT(friends).OUT(friends).OUT(friends).OUT(friends).TAG(people);
-- 5 度人脉查询
SELECT count(*) FROM people WHERE id=1234 TRAVERSE OUT(friends).OUT(friends).OUT(friends).OUT(friends).OUT(friends).TAG(people);
-- 6 度人脉查询
SELECT count(*) FROM people WHERE id=1234 TRAVERSE OUT(friends).OUT(friends).OUT(friends).OUT(friends).OUT(friends).OUT(friends).TAG(people);
```

## 对比 TiDB N 度人脉测试

1. 准备表的测试数据

将生成 的 `data.sql` 的前 4 行 SQL 换成以下 SQL 以创建表：

```sql
drop database if exists test2;
create database test2;
use test2;
drop table if exists people;
create table people (id bigint, name varchar(32));
drop table if exists friends;
create table friends (src bigint, dst bigint);
```

然后导入数据：

```shell
mysql -u root -h 127.0.0.1 -P 4000 test2 < data.sql
```

2. N 度人脉测试 SQL

```sql
use test2;
-- 2 度人脉查询
SELECT count(*) FROM people WHERE id IN (SELECT dst FROM friends WHERE src IN (SELECT dst FROM friends WHERE src = 1234) AND dst NOT IN (SELECT dst FROM friends WHERE src = 1234) AND src != 1234);

-- 3 度人脉查询
SELECT count(*) FROM people WHERE id IN (SELECT dst FROM friends WHERE src IN (SELECT dst FROM friends WHERE src IN (SELECT dst FROM friends WHERE src = 1234) AND dst NOT IN (SELECT dst FROM friends WHERE src = 1234) AND src != 1234) AND dst NOT IN (SELECT dst FROM friends WHERE src IN (SELECT dst FROM friends WHERE src = 1234) AND dst NOT IN (SELECT dst FROM friends WHERE src = 1234) AND src != 1234) AND src != 1234);

-- 4 度人脉查询
SELECT count(*) FROM people WHERE id IN (SELECT dst FROM friends WHERE src IN (SELECT dst FROM friends WHERE src IN (SELECT dst FROM friends WHERE src IN (SELECT dst FROM friends WHERE src = 1234) AND dst NOT IN (SELECT dst FROM friends WHERE src = 1234) AND src != 1234) AND dst NOT IN (SELECT dst FROM friends WHERE src IN (SELECT dst FROM friends WHERE src = 1234) AND dst NOT IN (SELECT dst FROM friends WHERE src = 1234) AND src != 1234) AND src != 1234) AND dst NOT IN (SELECT dst FROM friends WHERE src IN (SELECT dst FROM friends WHERE src IN (SELECT dst FROM friends WHERE src = 1234) AND dst NOT IN (SELECT dst FROM friends WHERE src = 1234) AND src != 1234) AND dst NOT IN (SELECT dst FROM friends WHERE src IN (SELECT dst FROM friends WHERE src = 1234) AND dst NOT IN (SELECT dst FROM friends WHERE src = 1234) AND src != 1234) AND src != 1234) AND src != 1234);
```
