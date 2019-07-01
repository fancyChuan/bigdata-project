package cn.fancychuan.dao.impl;

import cn.fancychuan.dao.ISessionDetailDAO;
import cn.fancychuan.domain.SessionDetail;
import cn.fancychuan.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

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

    @Override
    public void insertBatch(List<SessionDetail> sessionDetailList) {
        String sql = "insert into session_detail values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        List<Object[]> paramList = new ArrayList<>();
        for (SessionDetail sessionDetail : sessionDetailList) {
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
            paramList.add(params);
        }


        JDBCHelper instance = JDBCHelper.getInstance();
        instance.executeBatch(sql, paramList);
    }
}
