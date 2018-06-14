package com.bullhorn.orm.inmem.dao;

import com.bullhorn.orm.inmem.model.TblAzureConsumer;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AzureConsumerDAO extends CrudRepository<TblAzureConsumer,Long> {

}
