---------------------------------------------------------------------------------
--                                 插入标签的表
---------------------------------------------------------------------------------
drop table if exists dw.profile_tag_userid;
CREATE TABLE `dw.profile_tag_userid`(
`tagid` string COMMENT 'tagid', 
`userid` string COMMENT 'userid', 
`tagweight` string COMMENT 'tagweight', 
`reserve1` string COMMENT '预留1', 
`reserve2` string COMMENT '预留2', 
`reserve3` string COMMENT '预留3')
COMMENT 'tagid维度userid 用户画像数据'
PARTITIONED BY ( `data_date` string COMMENT '数据日期', `tagtype` string COMMENT '标签主题分类')
;

drop table if exists dw.profile_tag_cookieid;
CREATE TABLE `dw.profile_tag_cookieid`(
`tagid` string COMMENT 'tagid', 
`cookieid` string COMMENT 'cookieid', 
`tagweight` string COMMENT '标签权重', 
`reserve1` string COMMENT '预留1', 
`reserve2` string COMMENT '预留2', 
`reserve3` string COMMENT '预留3')
COMMENT 'tagid维度的用户cookie画像数据'
PARTITIONED BY ( `data_date` string COMMENT '数据日期', `tagtype` string COMMENT '标签主题分类')
;
---------------------------------------------------------------------------------
--                                标签转换的表
---------------------------------------------------------------------------------
drop table if exists `dw.profile_user_map_cookieid`;
CREATE TABLE `dw.profile_user_map_cookieid`(
`cookieid` string COMMENT 'tagid', 
`tagsmap` map<string,string> COMMENT 'cookieid', 
`reserve1` string COMMENT '预留1', 
`reserve2` string COMMENT '预留2')
COMMENT 'cookie 用户画像数据'
PARTITIONED BY (`data_date` string COMMENT '数据日期')
;

drop table if exists
CREATE TABLE `dw.profile_user_map_userid`(
`userid` string COMMENT 'userid', 
`tagsmap` map<string,string> COMMENT 'tagsmap', 
`reserve1` string COMMENT '预留1', 
`reserve2` string COMMENT '预留2')
COMMENT 'userid 用户画像数据'
PARTITIONED BY (`data_date` string COMMENT '数据日期')
;
--------------------------------------------------------
--                        人群 
--------------------------------------------------------
drop table if exists dw.profile_user_group;
CREATE TABLE `dw.profile_user_group`(
`id` string, 
`tagsmap` map<string,string>, 
`reserve` string, 
`reserve1` string)
PARTITIONED BY (`data_date` string, `target` string)
;

--------------------------------------------------------
--                    hive 向hbase映射数据
--------------------------------------------------------

hive --auxpath $HIVE_HOME/lib/zookeeper-3.4.6.jar,$HIVE_HOME/lib/hive-hbase-handler-1.2.1.jar,$HBASE_HOME/lib/hbase-server-1.3.1.jar --hiveconf hbase.zookeeper.quorum=hadoop101,hadoop102,hadoop103

drop table if exists dw.userprofile_hive_2_hbase;
CREATE TABLE dw.userprofile_hive_2_hbase
(key string, 
value string) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:val")
TBLPROPERTIES ("hbase.table.name" = "userprofile_hbase");

INSERT OVERWRITE TABLE dw.userprofile_hive_2_hbase 
SELECT userid,tagid FROM dw.profile_tag_userid;



