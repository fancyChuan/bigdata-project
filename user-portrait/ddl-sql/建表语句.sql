
drop table if exists dmp_book.userprofile_userlabel_map_all
create table `dmp_book.userprofile_userlabel_map_all`(
    `userid` string comment 'userid',
    `userlabels` map<string,string> comment 'tagsmap',)
comment 'userid 用户标签汇聚'
partitioned by ( `data_date` string comment '数据日期')
;


-- 这个表只考虑统计类型标签的权重，比如历史购买金额标签对应的权重为金额。
-- 该权重值的计算未考虑较为复杂的用户行为次数、行为类型、行为距今时间等复杂情况
drop table if exists dmp_book.userprofile_attritube_all;
create table `dw.userprofile_attritube_all `(
    `userid` string comment 'userid',
    `labelweight` string comment '标签权重')
comment 'userid 用户画像数据'
partitioned by ( `data_date` string comment '数据日期'
                , `theme` string comment '二级主题'
                , `labelid` string comment '标签id')
;

-- 比如用户当日浏览某三级品类商品3次
-- act_type_id行为类型（比如浏览、搜索、收藏、下单） tag_type_id标签类型（比如母婴、3C、数码等）
CREATE TABLE dw.userprofile_act_feature_append (
  labelid STRING COMMENT '标签id',
  cookieid STRING COMMENT '用户id',
  act_cnt int COMMENT '行为次数',
  tag_type_id int COMMENT '标签类型编码',
  act_type_id int COMMENT '行为类型编码')
comment '用户画像-用户行为标签表'
PARTITIONED BY (data_date STRING COMMENT '数据日期')
;