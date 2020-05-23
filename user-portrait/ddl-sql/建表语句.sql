
drop table if exists dmp_book.userprofile_userlabel_map_all
create table `dmp_book.userprofile_userlabel_map_all`(
`userid` string comment 'userid',
`userlabels` map<string,string> comment 'tagsmap',)
comment 'userid 用户标签汇聚'
partitioned by ( `data_date` string comment '数据日期')
;

