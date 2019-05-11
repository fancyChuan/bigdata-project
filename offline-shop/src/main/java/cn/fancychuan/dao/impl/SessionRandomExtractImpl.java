package cn.fancychuan.dao.impl;

import cn.fancychuan.dao.ISessionRandomExtractDAO;
import cn.fancychuan.domain.SessionRandomExtract;
import cn.fancychuan.jdbc.JDBCHelper;

public class SessionRandomExtractImpl implements ISessionRandomExtractDAO {
    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql = "insert into session_random_extract values (?, ?, ?, ?, ?, ?)";
        Object[] params = new Object[]{
                sessionRandomExtract.getTaskid(),
                sessionRandomExtract.getSessionid(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSearchKeywords(),
                sessionRandomExtract.getClickCategoryIds()
        };
        JDBCHelper instance = JDBCHelper.getInstance();
        instance.executeUpdate(sql, params);
    }
}
