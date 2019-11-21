

DROP TABLE IF EXISTS lineitem CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS partsupp CASCADE;
DROP TABLE IF EXISTS supplier CASCADE;
DROP TABLE IF EXISTS part CASCADE;
DROP TABLE IF EXISTS region CASCADE;
DROP TABLE IF EXISTS nation CASCADE;

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

@COPY region;
@COPY nation;
@COPY supplier;
@COPY customer;
@COPY part;
@COPY partsupp;
@COPY orders;
@COPY lineitem;

ALTER TABLE region   ADD PRIMARY KEY (r_regionkey);
ALTER TABLE nation   ADD PRIMARY KEY (n_nationkey);
ALTER TABLE nation   ADD CONSTRAINT nation_fk1 FOREIGN KEY (n_regionkey) REFERENCES region;
ALTER TABLE part     ADD PRIMARY KEY (p_partkey);
ALTER TABLE supplier ADD PRIMARY KEY (s_suppkey);
ALTER TABLE supplier ADD CONSTRAINT supplier_fk1 FOREIGN KEY (s_nationkey) REFERENCES nation;
ALTER TABLE partsupp ADD PRIMARY KEY (ps_partkey,ps_suppkey);
ALTER TABLE customer ADD PRIMARY KEY (c_custkey);
ALTER TABLE customer ADD CONSTRAINT customer_fk1 FOREIGN KEY (c_nationkey) REFERENCES nation;
ALTER TABLE lineitem ADD PRIMARY KEY (l_orderkey,l_linenumber);
ALTER TABLE orders   ADD PRIMARY KEY (o_orderkey);
ALTER TABLE partsupp ADD CONSTRAINT partsupp_fk1 FOREIGN KEY (ps_suppkey) REFERENCES supplier;
ALTER TABLE partsupp ADD CONSTRAINT partsupp_fk2 FOREIGN KEY (ps_partkey) REFERENCES part;
ALTER TABLE orders   ADD CONSTRAINT orders_fk1 FOREIGN KEY (o_custkey) REFERENCES customer;
ALTER TABLE lineitem ADD CONSTRAINT lineitem_fk1 FOREIGN KEY (l_orderkey) REFERENCES orders;
ALTER TABLE lineitem ADD CONSTRAINT lineitem_fk2 FOREIGN KEY (l_partkey,l_suppkey) REFERENCES partsupp;
