package com.bullhorn.orm.refreshWork.dao;

import com.bullhorn.orm.refreshWork.model.TblIntegrationServiceBusMessages;

import java.util.List;

public interface RefreshWorkDAOExt {
    void batchInsert(List<TblIntegrationServiceBusMessages> msgs);

    void batchInsertTest(List<String> msgs);
}
