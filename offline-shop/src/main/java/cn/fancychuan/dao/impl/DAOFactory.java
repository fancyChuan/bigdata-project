package cn.fancychuan.dao.impl;

import cn.fancychuan.dao.ISessionAggrStatDAO;
import cn.fancychuan.dao.ISessionRandomExtractDAO;
import cn.fancychuan.dao.ITaskDAO;

/**
 * DAO工厂类
 */
public class DAOFactory {
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }

    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAOImpl();
    }

    public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
        return new SessionRandomExtractImpl();
    }
}
