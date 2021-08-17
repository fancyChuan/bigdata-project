--标签聚合
insert overwrite table dw.profile_user_map_userid  partition(data_date="20180421")
select userid,
	   str_to_map(concat_ws(',',collect_set(concat(tagid,':',tagweight)))) as tagsmap,
	   '',
	   ''
from dw.profile_tag_userid
where data_date="20180421"
group by userid