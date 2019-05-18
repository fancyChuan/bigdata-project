package cn.fancychuan.dao.impl;

import cn.fancychuan.dao.*;

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

    public static ISessionDetailDAO getSessionDetailDAO() {
        return new SessionDetailImpl();
    }

    public static ITop10CategoryDAO getTop10CategoryDAO() {
        return new Top10CategoryDAOImpl();
    }
}
