package com.bullhorn;

import com.bullhorn.json.model.AzureConfig;
import com.bullhorn.orm.inmem.dao.AzureConsumerDAO;
import com.bullhorn.orm.refreshWork.dao.ServiceBusMessagesDAO;
import com.bullhorn.orm.timecurrent.dao.ErrorsDAO;
import com.bullhorn.orm.timecurrent.dao.FrontOfficeSystemDAO;
import com.bullhorn.services.AzureConsumerAsyncService;
import com.bullhorn.services.DataSwapper;
import com.microsoft.azure.servicebus.QueueClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
@EnableAutoConfiguration(exclude = { JacksonAutoConfiguration.class })
@EnableScheduling
public class AzureConsumerApplication {

	private static final Logger LOGGER = LoggerFactory.getLogger(AzureConsumerApplication.class);

	@Autowired
	Environment env;

	final FrontOfficeSystemDAO frontOfficeSystemDao;
	final ErrorsDAO errorsDAO;
	final AzureConsumerDAO azureConsumerDAO;
	final ServiceBusMessagesDAO serviceBusMessagesDAO;

	@Autowired
	public AzureConsumerApplication(FrontOfficeSystemDAO frontOfficeSystemDao, ErrorsDAO errorsDAO, AzureConsumerDAO azureConsumerDAO, ServiceBusMessagesDAO serviceBusMessagesDAO) {
		this.frontOfficeSystemDao = frontOfficeSystemDao;
		this.errorsDAO = errorsDAO;
		this.azureConsumerDAO = azureConsumerDAO;
		this.serviceBusMessagesDAO = serviceBusMessagesDAO;
	}

	public static void main(String[] args) {
		SpringApplication.run(AzureConsumerApplication.class, args);
	}

	@Bean(name = "azureConfig")
	public AzureConfig azureConfig(){
		AzureConfig config = new AzureConfig();
		config.setLstFOS(frontOfficeSystemDao.findByStatus(true));
		config.setTopicName(env.getProperty("azureconsumer.topicName"));
		LOGGER.debug("{}",config.toString());
		return  config;
	}


	@Bean("consumerTaskExecutor")
	@DependsOn("azureConfig")
	public TaskExecutor consumerTaskExecutor() {
		LOGGER.debug("Starting Consumer Task Executor");
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		//TODO: Refactor the Threadpool Size construct
		executor.setCorePoolSize(azureConfig().getLstFOS().size());
		executor.setMaxPoolSize(100);
		executor.setThreadNamePrefix("AZURE-CONSUMER-");
		//executor.initialize();
		return executor;
	}

	@Bean("swapperTaskExecutor")
	@DependsOn("consumerTaskExecutor")
	public TaskExecutor swapperTaskExecutor() {
		LOGGER.debug("Starting Swapper Task Executor");
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(1);
		executor.setMaxPoolSize(1);
		executor.setThreadNamePrefix("DATA-SWAPPER-");
		//executor.initialize();
		return executor;
	}

	private List<QueueClient> consumers = new ArrayList<>();

	@Bean(name = "consumer-async-svc")
	@DependsOn("consumerTaskExecutor")
	public AzureConsumerAsyncService azureConsumerAsycSvcInit() {
		LOGGER.debug("AzureConsumerAsyncService Constructed");
		return new AzureConsumerAsyncService(azureConfig(),azureConsumerDAO,errorsDAO);
	}

	@EventListener
	public void init(ContextRefreshedEvent event) {
		LOGGER.debug("Starting Azure Consumer");
		this.consumers = azureConsumerAsycSvcInit().executeAsynchronously();
		LOGGER.debug("Starting Data Swapper");
		DataSwapper dataSwapper = new DataSwapper(serviceBusMessagesDAO,azureConsumerDAO);
		dataSwapper.run();
	}

	@PreDestroy
	public void destroy() {
		LOGGER.debug("Shutting down Azure Consumers");
		consumers.forEach((consumer)->{
			try {
				consumer.close();
			} catch (Exception e){
				e.printStackTrace();
			}
		});
	}

}
