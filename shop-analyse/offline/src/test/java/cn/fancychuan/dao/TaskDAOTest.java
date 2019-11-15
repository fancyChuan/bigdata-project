package cn.fancychuan.dao;

import cn.fancychuan.dao.impl.DAOFactory;
import cn.fancychuan.domain.Task;

public class TaskDAOTest {
    public static void main(String[] args) {
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(1);
        System.out.println(task);
    }
}
