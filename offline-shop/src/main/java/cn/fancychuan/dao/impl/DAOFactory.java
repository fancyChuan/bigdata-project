package cn.fancychuan.dao.impl;

import cn.fancychuan.dao.ITaskDAO;

/**
 * DAO工厂类
 */
public class DAOFactory {
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }
}
