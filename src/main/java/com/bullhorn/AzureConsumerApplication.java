package com.bullhorn;

import com.bullhorn.orm.inmem.dao.AzureConsumerDAO;
import com.bullhorn.orm.refreshWork.dao.ServiceBusMessagesDAO;
import com.bullhorn.orm.timecurrent.dao.ConfigDAO;
import com.bullhorn.orm.timecurrent.dao.FrontOfficeSystemDAO;
import com.bullhorn.orm.timecurrent.model.TblIntegrationConfig;
import com.bullhorn.services.AzureConsumerAsyncService;
import com.bullhorn.services.DataSwapperAsyncService;
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
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@SpringBootApplication
@EnableAutoConfiguration(exclude = { JacksonAutoConfiguration.class })
@EnableScheduling
public class AzureConsumerApplication {

	private static final Logger LOGGER = LoggerFactory.getLogger(AzureConsumerApplication.class);

	private Environment env;

	public Environment getEnv() {
		return env;
	}

	@Autowired
	public void setEnv(Environment env) {
		this.env = env;
	}

	private final FrontOfficeSystemDAO frontOfficeSystemDao;
    private final AzureConsumerDAO azureConsumerDAO;
    private final ServiceBusMessagesDAO serviceBusMessagesDAO;
    private final ConfigDAO configDAO;

    @Autowired
	public AzureConsumerApplication(FrontOfficeSystemDAO frontOfficeSystemDao, AzureConsumerDAO azureConsumerDAO, ServiceBusMessagesDAO serviceBusMessagesDAO, ConfigDAO configDAO) {
		this.frontOfficeSystemDao = frontOfficeSystemDao;
		this.azureConsumerDAO = azureConsumerDAO;
		this.serviceBusMessagesDAO = serviceBusMessagesDAO;
		this.configDAO = configDAO;
	}

	public static void main(String[] args) {
		SpringApplication.run(AzureConsumerApplication.class, args);
	}

	@Bean(name = "azureConsumerConfig")
	public List<TblIntegrationConfig> getConfig(){
		return configDAO.findAll();
	}


	@Bean("consumerTaskExecutor")
	@DependsOn("azureConsumerConfig")
	public TaskExecutor consumerTaskExecutor() {
		LOGGER.debug("Starting Consumer Task Executor");
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		TblIntegrationConfig val = getConfig().stream().filter((k) -> k.getCfgKey().equals("AZURE_CONSUMER_POOL_SIZE")).collect(Collectors.toList()).get(0);
		int poolSize = Integer.parseInt(val.getCfgValue());
		executor.setCorePoolSize(poolSize);
		executor.setMaxPoolSize(100);
		executor.setThreadNamePrefix("AZURE-CONSUMER-");
		return executor;
	}

	private List<QueueClient> consumers = new ArrayList<>();

	@Bean(name = "consumerAsyncSvc")
	@DependsOn("consumerTaskExecutor")
	public AzureConsumerAsyncService azureConsumerAsycSvcInit() {
		LOGGER.debug("AzureConsumerAsyncService Constructed");
		TblIntegrationConfig val = getConfig().stream().filter((k) -> k.getCfgKey().equals("AZURE_CONSUMER_QUEUE_NAME")).collect(Collectors.toList()).get(0);
		String queueName = val.getCfgValue();
		AzureConsumerAsyncService azureConsumerAsyncService = new AzureConsumerAsyncService(azureConsumerDAO);
		azureConsumerAsyncService.setLstFOS(frontOfficeSystemDao.findByStatus(true));
		azureConsumerAsyncService.setQueueName(queueName);
		return azureConsumerAsyncService;
	}

	@Bean("swapperTaskScheduler")
	@DependsOn("consumerAsyncSvc")
	public ThreadPoolTaskScheduler swapperTaskExecutor() {
		LOGGER.debug("Starting Swapper Task Scheduler");
		ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
		TblIntegrationConfig val = getConfig().stream().filter((k) -> k.getCfgKey().equals("DATA_SWAPPER_POOL_SIZE")).collect(Collectors.toList()).get(0);
		int poolSize = Integer.parseInt(val.getCfgValue());
		threadPoolTaskScheduler.setPoolSize(poolSize);
		threadPoolTaskScheduler.setThreadNamePrefix("DATA-SWAPPER-");
		return threadPoolTaskScheduler;
	}

	@Bean("dataSwapperAsyncSvc")
	@DependsOn("consumerAsyncSvc")
	public DataSwapperAsyncService dataSwapperAsyncServiceInit(){
		LOGGER.debug("DataSwapperAsyncService Constructed");
		TblIntegrationConfig val = getConfig().stream().filter((k) -> k.getCfgKey().equals("DATA_SWAPPER_EXECUTE_INTERVAL")).collect(Collectors.toList()).get(0);
		long interval = Long.parseLong(val.getCfgValue());
		DataSwapperAsyncService dataSwapperAsyncService = new DataSwapperAsyncService(serviceBusMessagesDAO,azureConsumerDAO);
		dataSwapperAsyncService.setInterval(interval);
		return dataSwapperAsyncService;
	}

	@EventListener
	public void init(ContextRefreshedEvent event) {
		LOGGER.debug("Starting Azure Consumer");
		this.consumers = azureConsumerAsycSvcInit().executeAsynchronously();
		LOGGER.debug("Starting Data Swapper");
		dataSwapperAsyncServiceInit().executeAsynchronously();
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
