package com.bullhorn;

import com.bullhorn.json.model.AzureConfig;
import com.bullhorn.orm.inmem.dao.AzureConsumerDAO;
import com.bullhorn.orm.timecurrent.dao.ErrorsDAO;
import com.bullhorn.orm.timecurrent.dao.FrontOfficeSystemDAO;
import com.bullhorn.services.AzureConsumerAsyncService;
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
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
@EnableAutoConfiguration(exclude = { JacksonAutoConfiguration.class })
public class AzureConsumerApplication {

	private static final Logger LOGGER = LoggerFactory.getLogger(AzureConsumerApplication.class);

	@Autowired
	Environment env;

	final FrontOfficeSystemDAO frontOfficeSystemDao;
	final ErrorsDAO errorsDAO;
	final AzureConsumerDAO azureConsumerDAO;

	@Autowired
	public AzureConsumerApplication(FrontOfficeSystemDAO frontOfficeSystemDao, ErrorsDAO errorsDAO, AzureConsumerDAO azureConsumerDAO) {
		this.frontOfficeSystemDao = frontOfficeSystemDao;
		this.errorsDAO = errorsDAO;
		this.azureConsumerDAO = azureConsumerDAO;
	}

	public static void main(String[] args) {
		SpringApplication.run(AzureConsumerApplication.class, args);
	}

	@Bean(name = "azureConfig")
	public AzureConfig azureConfig(){
		AzureConfig config = new AzureConfig();
		config.setLstFOS(frontOfficeSystemDao.findByStatus(true));
		config.setTopicName(env.getProperty("azureconsumer.topicName"));
		LOGGER.info("{}",config.toString());
		return  config;
	}


	@Bean("taskExecutor")
	@DependsOn("azureConfig")
	public TaskExecutor threadPoolTaskExecutor() {
		LOGGER.info("Starting Task Executor");
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		//TODO: Refactor the Threadpool Size construct
		executor.setCorePoolSize(azureConfig().getLstFOS().size());
		executor.setMaxPoolSize(100);
		executor.setThreadNamePrefix("AZURE-CONSUMER-");
		//executor.initialize();
		return executor;
	}

	private List<QueueClient> consumers = new ArrayList<>();

	@Bean(name = "consumer-async-svc")
	@DependsOn("taskExecutor")
	public AzureConsumerAsyncService azureConsumerAsycSvcInit() {
		LOGGER.debug("AzureConsumerAsyncService Constructed");
		return new AzureConsumerAsyncService(azureConfig(),azureConsumerDAO,errorsDAO);
	}


	@EventListener
	public void init(ContextRefreshedEvent event) {
		LOGGER.info("Starting Azure Consumer");
		this.consumers = azureConsumerAsycSvcInit().executeAsynchronously();
	}

	@PreDestroy
	public void destroy() {
		LOGGER.info("Shutting down Azure Consumers");
		consumers.forEach((consumer)->{
			try {
				consumer.close();
			} catch (Exception e){
				e.printStackTrace();
			}
		});
	}

}
