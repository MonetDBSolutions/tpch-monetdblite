select
	cntrycode,
	count(1) as numcust,
	sum(c_acctbal) as totacctbal
from (
	select
		cntrycode,
		c_acctbal,
		avg_acctbal
	from
		q22_customer_tmp1_cached ct1 join (
			select
				cntrycode,
				c_acctbal
			from
				q22_orders_tmp_cached ot
				right outer join q22_customer_tmp_cached ct
				on ct.c_custkey = ot.o_custkey
			where
				o_custkey is null
		) ct2
) a
where
	c_acctbal > avg_acctbal
group by
	cntrycode
order by
	cntrycode;
