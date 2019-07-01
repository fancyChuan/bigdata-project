package cn.fancychuan.dao;

import cn.fancychuan.domain.SessionDetail;

import java.util.List;

public interface ISessionDetailDAO {

    void insert(SessionDetail sessionDetail);

    void insertBatch(List<SessionDetail> sessionDetail);
}
