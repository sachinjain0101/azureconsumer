package com.bullhorn.orm.refreshWork.dao;

import com.bullhorn.orm.refreshWork.model.TblIntegrationServiceBusMessages;
import com.bullhorn.orm.timecurrent.dao.TimeCurrentDAOExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.PersistenceContext;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;

public class ServiceBusMessagesDAOExtImpl implements ServiceBusMessagesDAOExt {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeCurrentDAOExt.class);

    @Autowired
    @Qualifier("refreshWorkJdbcTemplate")
    JdbcTemplate jdbcTemplate;

    @Override
    public void batchInsert(List<TblIntegrationServiceBusMessages> msgs) {
        int[] updateCounts = jdbcTemplate.batchUpdate(
                "INSERT INTO tblIntegration_ServiceBusMessages (FrontOfficeSystemRecordID, Message, SequenceNumber, IntegrationKey, MessageID) values (?, ?, ?, ?, ?)",
                new BatchPreparedStatementSetter() {
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        ps.setInt(1, msgs.get(i).getFrontOfficeSystemRecordID());
                        ps.setString(2, msgs.get(i).getMessage());
                        ps.setLong(3, msgs.get(i).getSequenceNumber());
                        ps.setString(4,msgs.get(i).getIntegrationKey());
                        ps.setString(5, msgs.get(i).getMessageID());
                    }

                    public int getBatchSize() {
                        return msgs.size();
                    }
                } );
    }

//
//    @Override
//    public void batchInsert(List<TblIntegrationServiceBusMessages> msgs) {
//        int batchSize = 1000;
//        int size = msgs.size();
//
//
//        jdbcTemplate.ba
//
//
//        EntityTransaction txn = em.getTransaction();
//
//        refershWrokTransactionTemplate.execute(new TransactionCallbackWithoutResult() {
//            @Override
//            protected void doInTransactionWithoutResult(TransactionStatus status) {
//                for (int i = 0; i < size; i++) {
//                    if (i > 0 && i % batchSize == 0) {
//                        txn.commit();
//                        txn.begin();
//
//                        em.clear();
//                    }
//                    em.persist(msgs.get(i));
//                }
//            }
//        });
//
//
//            for (int i = 0; i < size; i++) {
//                if (i > 0 && i % batchSize == 0) {
//                    txn.commit();
//                    txn.begin();
//
//                    em.clear();
//                }
//                em.persist(msgs.get(i));
//            }
//    }

    /*
int entityCount = 50;
int batchSize = 25;

EntityManager entityManager = entityManagerFactory()
    .createEntityManager();

EntityTransaction entityTransaction = entityManager
    .getTransaction();

try {
    entityTransaction.begin();

    for (int i = 0; i < entityCount; i++) {
        if (i > 0 && i % batchSize == 0) {
            entityTransaction.commit();
            entityTransaction.begin();

            entityManager.clear();
        }

        Post post = new Post(
            String.format("Post %d", i + 1)
        );

        entityManager.persist(post);
    }

    entityTransaction.commit();
} catch (RuntimeException e) {
    if (entityTransaction.isActive()) {
        entityTransaction.rollback();
    }
    throw e;
} finally {
    entityManager.close();
}     */
}
