select
	ps_partkey, part_value as value
from (
	select
		ps_partkey,
		part_value,
		total_value
	from
		q11_part_tmp_cached join q11_sum_tmp_cached
) a
where
	part_value > total_value * @REPLACE_ME@
order by
	value desc;
