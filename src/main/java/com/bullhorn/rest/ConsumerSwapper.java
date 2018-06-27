package com.bullhorn.rest;

import com.bullhorn.orm.inmem.dao.AzureConsumerDAO;
import com.bullhorn.orm.inmem.model.TblAzureConsumer;
import com.bullhorn.services.SwapperHandler;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.stream.Collectors;

@RestController
@Api(value = "Base resource for Consumer")
@RequestMapping("/azureConsumer")
public class ConsumerSwapper {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerSwapper.class);

    private final AzureConsumerDAO azureConsumerDAO;
    private final TaskScheduler scheduler;

    private final SwapperHandler swapperHandler;

    @Autowired
    public ConsumerSwapper(AzureConsumerDAO azureConsumerDAO, @Qualifier("swapperTaskScheduler") TaskScheduler taskScheduler
			, @Qualifier("swapperHandler") SwapperHandler swapperHandler) {
        this.azureConsumerDAO = azureConsumerDAO;
        this.scheduler = taskScheduler;
        this.swapperHandler = swapperHandler;
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

	@ApiOperation(value="Test to see Azure ConsumerSwapper is working or not.")
	@RequestMapping(value = "/test", method = RequestMethod.GET)
	public String test() {
		return "Opera Azure ConsumerSwapper is running...";
	}

	@ApiOperation(value="Gets the Azure ConsumerSwapper thread information.")
	@RequestMapping(value = "/threads", method = RequestMethod.GET)
	public List<String> threads(){
		Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
		Thread[] threadArray = threadSet.toArray(new Thread[threadSet.size()]);
		List<String> lst = new ArrayList<>();
		for(Thread t:threadArray){
			lst.add(t.getName()+" : "+t.getState().toString());
		}
		return lst.stream().filter((s)->s.startsWith("DATA-SWAPPER")||s.startsWith("AZURE-CONSUMER")).collect(Collectors.toList());
	}

	@ApiOperation(value="Cancels the Swapper threads")
	@RequestMapping(value = "/cancelSwappers", method = RequestMethod.GET)
	public String cancelSwappers(){
		swapperHandler.shutdown();
		return "DONE";
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
