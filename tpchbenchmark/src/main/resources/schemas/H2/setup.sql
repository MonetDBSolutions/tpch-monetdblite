CREATE TABLE nation ( n_nationkey  INTEGER NOT NULL,
                      n_name       CHAR(25) NOT NULL,
                      n_regionkey  INTEGER NOT NULL,
                      n_comment    VARCHAR(152));

CREATE TABLE region ( r_regionkey  INTEGER NOT NULL,
                      r_name       CHAR(25) NOT NULL,
                      r_comment    VARCHAR(152));

CREATE TABLE part ( p_partkey     INTEGER NOT NULL,
                    p_name        VARCHAR(55) NOT NULL,
                    p_mfgr        CHAR(25) NOT NULL,
                    p_brand       CHAR(10) NOT NULL,
                    p_type        VARCHAR(25) NOT NULL,
                    p_size        INTEGER NOT NULL,
                    p_container   CHAR(10) NOT NULL,
                    p_retailprice DECIMAL(15,2) NOT NULL,
                    p_comment     VARCHAR(23) NOT NULL);

CREATE TABLE supplier ( s_suppkey     INTEGER NOT NULL,
                        s_name        CHAR(25) NOT NULL,
                        s_address     VARCHAR(40) NOT NULL,
                        s_nationkey   INTEGER NOT NULL,
                        s_phone       CHAR(15) NOT NULL,
                        s_acctbal     DECIMAL(15,2) NOT NULL,
                        s_comment     VARCHAR(101) NOT NULL);

CREATE TABLE partsupp ( ps_partkey     INTEGER NOT NULL,
                        ps_suppkey     INTEGER NOT NULL,
                        ps_availqty    INTEGER NOT NULL,
                        ps_supplycost  DECIMAL(15,2)  NOT NULL,
                        ps_comment     VARCHAR(199) NOT NULL);

CREATE TABLE customer ( c_custkey     INTEGER NOT NULL,
                        c_name        VARCHAR(25) NOT NULL,
                        c_address     VARCHAR(40) NOT NULL,
                        c_nationkey   INTEGER NOT NULL,
                        c_phone       CHAR(15) NOT NULL,
                        c_acctbal     DECIMAL(15,2) NOT NULL,
                        c_mktsegment  CHAR(10) NOT NULL,
                        c_comment     VARCHAR(117) NOT NULL);

CREATE TABLE orders ( o_orderkey       INTEGER NOT NULL,
                      o_custkey        INTEGER NOT NULL,
                      o_orderstatus    CHAR(1) NOT NULL,
                      o_totalprice     DECIMAL(15,2) NOT NULL,
                      o_orderdate      DATE NOT NULL,
                      o_orderpriority  CHAR(15) NOT NULL,
                      o_clerk          CHAR(15) NOT NULL,
                      o_shippriority   INTEGER NOT NULL,
                      o_comment        VARCHAR(79) NOT NULL);

CREATE TABLE lineitem ( l_orderkey      INTEGER NOT NULL,
                        l_partkey       INTEGER NOT NULL,
                        l_suppkey       INTEGER NOT NULL,
                        l_linenumber    INTEGER NOT NULL,
                        l_quantity      DECIMAL(15,2) NOT NULL,
                        l_extendedprice DECIMAL(15,2) NOT NULL,
                        l_discount      DECIMAL(15,2) NOT NULL,
                        l_tax           DECIMAL(15,2) NOT NULL,
                        l_returnflag    CHAR(1) NOT NULL,
                        l_linestatus    CHAR(1) NOT NULL,
                        l_shipdate      DATE NOT NULL,
                        l_commitdate    DATE NOT NULL,
                        l_receiptdate   DATE NOT NULL,
                        l_shipinstruct  CHAR(25) NOT NULL,
                        l_shipmode      CHAR(10) NOT NULL,
                        l_comment       VARCHAR(44) NOT NULL);

-- tpch-02
create view q2_min_ps_supplycost as
select p_partkey as min_p_partkey, min(ps_supplycost) as min_ps_supplycost
from part, partsupp, supplier, nation, region where p_partkey = ps_partkey
and s_suppkey = ps_suppkey and s_nationkey = n_nationkey
and n_regionkey = r_regionkey and r_name = 'EUROPE' group by p_partkey;

-- tpch-11
create view q11_part_tmp_cached as
select ps_partkey, sum(ps_supplycost * ps_availqty) as part_value
from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey
and n_name = 'GERMANY' group by ps_partkey;

create view q11_sum_tmp_cached as
select sum(part_value) as total_value from q11_part_tmp_cached;

-- tpch-15
create view revenue_cached as
select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue
from lineitem where l_shipdate >= '1996-01-01' and l_shipdate < '1996-04-01' group by l_suppkey;

create view max_revenue_cached as
select max(total_revenue) as max_revenue from revenue_cached;

-- tpch-18
create view q18_tmp_cached as
select l_orderkey, sum(l_quantity) as t_sum_quantity
from lineitem where l_orderkey is not null group by l_orderkey;

-- tpch-22
create view q22_customer_tmp_cached as
select c_acctbal, c_custkey, substr(c_phone, 1, 2) as cntrycode
from customer where substring(c_phone from 1 for 2) in ('13', '31', '23', '29', '30', '18', '17');

create view q22_customer_tmp1_cached as
select avg(c_acctbal) as avg_acctbal from q22_customer_tmp_cached where c_acctbal > 0.00;

create view q22_orders_tmp_cached as
select o_custkey from orders group by o_custkey;

INSERT INTO region   SELECT * FROM CSVREAD('@DATAPATH@/region.tbl',   NULL, 'charset=UTF-8 fieldSeparator=| lineSeparator=\n');
INSERT INTO nation   SELECT * FROM CSVREAD('@DATAPATH@/nation.tbl',   NULL, 'charset=UTF-8 fieldSeparator=| lineSeparator=\n');
INSERT INTO supplier SELECT * FROM CSVREAD('@DATAPATH@/supplier.tbl', NULL, 'charset=UTF-8 fieldSeparator=| lineSeparator=\n');
INSERT INTO customer SELECT * FROM CSVREAD('@DATAPATH@/customer.tbl', NULL, 'charset=UTF-8 fieldSeparator=| lineSeparator=\n');
INSERT INTO part     SELECT * FROM CSVREAD('@DATAPATH@/part.tbl',     NULL, 'charset=UTF-8 fieldSeparator=| lineSeparator=\n');
INSERT INTO partsupp SELECT * FROM CSVREAD('@DATAPATH@/partsupp.tbl', NULL, 'charset=UTF-8 fieldSeparator=| lineSeparator=\n');
INSERT INTO orders   SELECT * FROM CSVREAD('@DATAPATH@/orders.tbl',   NULL, 'charset=UTF-8 fieldSeparator=| lineSeparator=\n');
INSERT INTO lineitem SELECT * FROM CSVREAD('@DATAPATH@/lineitem.tbl', NULL, 'charset=UTF-8 fieldSeparator=| lineSeparator=\n');
