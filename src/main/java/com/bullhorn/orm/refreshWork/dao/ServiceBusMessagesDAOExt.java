package com.bullhorn.orm.refreshWork.dao;

import com.bullhorn.orm.refreshWork.model.TblIntegrationServiceBusMessages;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface ServiceBusMessagesDAOExt {
    void batchInsert(List<TblIntegrationServiceBusMessages> msgs);
}
