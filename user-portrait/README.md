## 用户画像系统

用户画像建模就是对用户“打标签”，有三种类型；
- 统计类：性别、年龄、近7天活跃时长、活跃天数等
- 规则类：比如“消费活跃”用户的口径定义为“近30天交易次数>2”（运营人员和数据人员共同协商确定）
- 机器学习数据挖掘类：比如购买商品偏好、用户流失意向等（成本高，一般这类标签占比较少）

![image](images/用户画像数仓架构.png)

用户画像模块建设

![image](images/用户画像主要覆盖模块.jpg)

![image](images/用户画像建设项目流程.jpg)

![image](images/用户画像项目各阶段关键产出.jpg)


### 案例：图书电商
商城的运营目标：
- 兼顾自身商业目标的同时更好的满足消费者的需求，通过推荐系统提高点击率等
- 建立用户流失预警机制，及时级别将要流失的用户群体，采用运营策略挽回用户

表：
- 用户信息表 dim.user_basic_info
- 商品订单表 dw.order_info_fact 
- 埋点日志表 ods.page_event_log
- 访问日志表 ods.page_view_log
- 商品评价表 dw.book_comment
- 搜索日志表 dw.app_search_log
- 用户收藏表 dw.book_collection_df
- 购物车信息表 dw.shopping_cart_df

宽表设计：
- 用户属性宽表
- 用户日活跃宽表
