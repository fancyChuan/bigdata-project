package cn.fancychuan.dao;

import cn.fancychuan.domain.Task;

/**
 * 任务管理DAO接口
 */
public interface ITaskDAO {
    Task findById(long taskid);
}
