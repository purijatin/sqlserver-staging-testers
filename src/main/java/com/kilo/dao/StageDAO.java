
package com.kilo.dao;

import java.util.List;

import com.kilo.domain.MotleyObject;
import com.kilo.domain.StageResult;

public interface StageDAO {

    /**
     * Stages the given records using the given template table in the template
     * DB
     * 
     * @param records
     *            given records
     * @param templateDB
     * @param templateTable
     * @return stage result containing details of where the data was staged
     */
    StageResult stage(List<MotleyObject> records, String templateDB,
            String templateTable);

    void dropStageTable(StageResult stageResult);
}
