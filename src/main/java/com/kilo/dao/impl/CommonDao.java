package com.kilo.dao.impl;


import java.util.HashMap;

import org.mybatis.spring.support.SqlSessionDaoSupport;
import org.springframework.transaction.annotation.Transactional;

import com.kilo.domain.RunStats;

public class CommonDao extends SqlSessionDaoSupport{

    @Transactional
    public void insert(RunStats stats){
        HashMap<String, Object> map = new HashMap<>();
        map.put("rec", stats);
        map.put("runID", stats.getRunID());
        map.put("runType", stats.getRunType());
        map.put("success", stats.isSuccess());
        map.put("startTime", stats.getStartTime());
        map.put("endTime", stats.getEndTime());
        map.put("timeTaken", stats.getTimeTaken());
        stats.dbInnerTime.ifPresent(x -> map.put("dbInner", x));
        stats.networkTime.ifPresent(x -> map.put("network", x));
        stats.tableCreationTime.ifPresent(x -> map.put("tableCreation", x));
        stats.tableDropTime.ifPresent(x -> map.put("tableDrop", x));

        getSqlSession().insert("com.kilo.dao.mybatis.mapper.Motley.insertStats", map);
        getSqlSession().flushStatements();
    }


}
