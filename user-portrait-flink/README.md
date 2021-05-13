## 基于Flink流处理的动态实时亿级全端用户画像系统




```
CREATE TABLE userinfo (
userid INT(20) COMMENT '用户ID',
username VARCHAR(50) COMMENT '用户名',
PASSWORD VARCHAR(50) COMMENT '密码',
sex INT(1) COMMENT '性别',
telphone VARCHAR(50) COMMENT '手机号',
email VARCHAR(50) COMMENT '邮箱',
age INT(20) COMMENT '年龄',
idcard VARCHAR(50) COMMENT '身份证',
registerTime TIMESTAMP COMMENT '注册时间',
useType INT(1) COMMENT '终端类型：0、PC端；1、移动端；2、小程序端'
) COMMENT '用户基本信息表'
```