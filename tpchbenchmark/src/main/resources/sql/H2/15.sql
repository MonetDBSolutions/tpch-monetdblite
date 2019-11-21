select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	supplier,
	revenue_cached,
	max_revenue_cached
where
	s_suppkey = supplier_no
	and total_revenue = max_revenue 
order by s_suppkey;
