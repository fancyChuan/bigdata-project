package cn.fancychuan.dao.impl;

import cn.fancychuan.dao.ITop10CategoryDAO;
import cn.fancychuan.domain.Top10Category;
import cn.fancychuan.jdbc.JDBCHelper;

public class Top10CategoryDAOImpl implements ITop10CategoryDAO {
    @Override
    public void insert(Top10Category top10Category) {
        String sql = "insert into top10_category values(?, ?, ?, ?, ?)";

        JDBCHelper instance = JDBCHelper.getInstance();

        Object[] params = new Object[] {
                top10Category.getTaskid(),
                top10Category.getCategoryid(),
                top10Category.getClickCount(),
                top10Category.getOrderCount(),
                top10Category.getPayCount()
        };

        instance.executeUpdate(sql, params);
    }
}
