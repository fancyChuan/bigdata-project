package cn.fancychuan.dao.impl;

import cn.fancychuan.dao.ISessionDetailDAO;
import cn.fancychuan.domain.SessionDetail;
import cn.fancychuan.jdbc.JDBCHelper;

public class SessionDetailImpl implements ISessionDetailDAO {
    @Override
    public void insert(SessionDetail sessionDetail) {
        String sql = "insert into session_detail values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Object[] params = new Object[] {
                sessionDetail.getTaskid(),
                sessionDetail.getUserid(),
                sessionDetail.getSessionid(),
                sessionDetail.getPageid(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickProductId(),
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPayCategoryIds(),
                sessionDetail.getPayProductIds()
        };

        JDBCHelper instance = JDBCHelper.getInstance();
        instance.executeUpdate(sql, params);
    }
}
