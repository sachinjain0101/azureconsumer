package com.bullhorn.rest;

import com.bullhorn.orm.inmem.dao.AzureConsumerDAO;
import com.bullhorn.orm.inmem.model.TblAzureConsumer;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@RestController
@Api(value = "Base resource for Opera-DataMapper")
@RequestMapping("/maps")
public class Consumer {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    private final AzureConsumerDAO azureConsumerDAO;

    @Autowired
    public Consumer(AzureConsumerDAO azureConsumerDAO) {
        this.azureConsumerDAO = azureConsumerDAO;
    }

	@ApiOperation(value="Check count in InMemory database")
	@RequestMapping(value = "/consumedCount", method = RequestMethod.GET)
	public Integer consumedCount() {
		Iterator<TblAzureConsumer> itr =  azureConsumerDAO.findAll().iterator();
		List<TblAzureConsumer> lst = new ArrayList<>();
		while (itr.hasNext()){
			TblAzureConsumer a = itr.next();
			lst.add(a);
		}
		return lst.size();
	}

	@ApiOperation(value="Test to see Azure Consumer is working or not.")
	@RequestMapping(value = "/test", method = RequestMethod.GET)
	public String test() {
		return "Opera Azure Consumer is running...";
	}

}

/*
    {
        "client": "SOME",
        "data": [{
            "EmployeeFirstName": "Sachin WhatUp",
            "EmployeeLastName": "Jain",
            "EmployeeID": "1234",
            "EmployeeSSN": "987654321",
            "Codes": {
                "X1": "Y1",
                "X2": "Y2"
            }
        }, {
            "EmployeeFirstName": "Shalina",
            "EmployeeLastName": "Jain",
            "EmployeeID": "",
            "EmployeeSSN": "98989898",
            "Codes": {
                "X1": "Y1"
            }
        }],
        "integrationKey": "12345",
        "mapName": "Test",
        "messageId": "67890"
    }
*/
