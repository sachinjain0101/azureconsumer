package com.bullhorn;

import com.bullhorn.app.Constants;
import com.bullhorn.orm.inmem.dao.AzureConsumerDAO;
import com.bullhorn.orm.refreshWork.dao.ServiceBusMessagesDAO;
import com.bullhorn.orm.timecurrent.dao.ConfigDAO;
import com.bullhorn.orm.timecurrent.dao.FrontOfficeSystemDAO;
import com.bullhorn.orm.timecurrent.model.TblIntegrationConfig;
import com.bullhorn.orm.timecurrent.model.TblIntegrationFrontOfficeSystem;
import com.bullhorn.services.ConsumerHandler;
import com.bullhorn.services.SwapperHandler;
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

	private List<TblIntegrationFrontOfficeSystem> lstFOS = null;

	@Bean(name = "integrationConfig")
	public List<TblIntegrationConfig> getConfig(){
		String cluster = (env.getProperty("azureConsumer.clusterName")!=null)?env.getProperty("azureConsumer.clusterName"):"";
		LOGGER.debug("Cluster info : {} & isEmpty : {}",cluster,cluster.isEmpty());
		// VIMP: lstFOS drives the AzureConsumer ThreadPool size
		lstFOS = frontOfficeSystemDao.findByStatus(true,cluster);
    	return configDAO.findAll();
	}


	@Bean("consumerTaskExecutor")
	@DependsOn("integrationConfig")
	public ThreadPoolTaskExecutor consumerTaskExecutor() {
		LOGGER.debug("Starting ConsumerSwapper Task Executor");
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(lstFOS.size());
		executor.setMaxPoolSize(100);
		return executor;
	}

	private List<QueueClient> consumers = new ArrayList<>();

	@Bean(name = "consumerHandler")
	@DependsOn("consumerTaskExecutor")
	public ConsumerHandler consumerHandler() {
		LOGGER.debug("ConsumerHandler Constructed");
		TblIntegrationConfig val = getConfig().stream().filter((k) -> k.getCfgKey().equals(Constants.AZURE_CONSUMER_QUEUE_NAME)).collect(Collectors.toList()).get(0);
		String queueName = val.getCfgValue();
		ConsumerHandler consumerHandler = new ConsumerHandler(azureConsumerDAO);
		consumerHandler.setLstFOS(lstFOS);
		consumerHandler.setQueueName(queueName);
		return consumerHandler;
	}

	@Bean("swapperTaskScheduler")
	@DependsOn("consumerHandler")
	public ThreadPoolTaskScheduler swapperTaskScheduler() {
		LOGGER.debug("Starting Swapper Task Scheduler");
		ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
		threadPoolTaskScheduler.setPoolSize(lstFOS.size());
		threadPoolTaskScheduler.setWaitForTasksToCompleteOnShutdown(true);
		TblIntegrationConfig val2 = getConfig().stream().filter((k) -> k.getCfgKey().equals(Constants.DATA_SWAPPER_THREADPOOL_SCHEDULER_TERMINATION_TIME_INSECONDS)).collect(Collectors.toList()).get(0);
		int terminationTime = Integer.parseInt(val2.getCfgValue());
		threadPoolTaskScheduler.setAwaitTerminationSeconds(terminationTime);
		threadPoolTaskScheduler.setThreadNamePrefix("DATA-SWAPPER-");
		return threadPoolTaskScheduler;
	}

	@Bean("swapperHandler")
	@DependsOn("swapperTaskScheduler")
	public SwapperHandler swapperHandler(){
		LOGGER.debug("SwapperHandler Constructed");
		TblIntegrationConfig val1 = getConfig().stream().filter((k) -> k.getCfgKey().equals(Constants.DATA_SWAPPER_EXECUTE_INTERVAL)).collect(Collectors.toList()).get(0);
		long interval = Long.parseLong(val1.getCfgValue());
        SwapperHandler swapperHandler = new SwapperHandler(serviceBusMessagesDAO,azureConsumerDAO);
		swapperHandler.setInterval(interval);
		//Swapper poolSize has to be 1 because it is just reponsible for swapping data and it is not suppoed to have any special logic
		swapperHandler.setPoolSize(1);
		return swapperHandler;
	}

	@EventListener
	public void init(ContextRefreshedEvent event) {
		LOGGER.debug("Starting Azure ConsumerSwapper");
		this.consumers = consumerHandler().executeAsynchronously();
		LOGGER.debug("Starting Data Swapper");
		swapperHandler().executeAsynchronously();
		addDataSwapperShutdownHook();
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

	public void addDataSwapperShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				LOGGER.info("Shutdown received");
				swapperHandler().shutdown();
			}
		});

		Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
			@Override
			public void uncaughtException(Thread t, Throwable e) {
				LOGGER.error("Uncaught Exception on " + t.getName() + " : " + e, e);
				swapperHandler().shutdown();
			}
		});
		LOGGER.info("Data Swapper ShutdownHook Added");
	}


}
