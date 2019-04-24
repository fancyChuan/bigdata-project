package cn.fancychuan.dao.impl;

import cn.fancychuan.dao.ITaskDAO;
import cn.fancychuan.domain.Task;
import cn.fancychuan.jdbc.JDBCHelper;

import java.sql.ResultSet;

public class TaskDAOImpl implements ITaskDAO {

    @Override
    public Task findById(long taskid) {
        final Task task = new Task();

        String sql = "select * from task where task_id=?";
        Object[] params = {taskid};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryClassback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()) {
                    long taskid = rs.getLong(1);
                    String taskName = rs.getString(2);
                    String createTime = rs.getString(3);
                    String startTime = rs.getString(4);
                    String finishTime = rs.getString(5);
                    String taskType = rs.getString(6);
                    String taskStatus = rs.getString(7);
                    String taskParam = rs.getString(8);

                    task.setTaskid(taskid);
                    task.setTaskName(taskName);
                    task.setCreateTime(createTime);
                    task.setStartTime(startTime);
                    task.setFinishTime(finishTime);
                    task.setTaskType(taskType);
                    task.setTaskStatus(taskStatus);
                    task.setTaskParam(taskParam);
                }
            }
        });

        return null;
    }
}
