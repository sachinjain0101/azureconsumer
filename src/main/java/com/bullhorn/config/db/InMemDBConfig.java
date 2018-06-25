package com.bullhorn.config.db;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.HashMap;

@Configuration
@PropertySource({"file:orm-multi-db.properties"})
@EnableJpaRepositories(basePackages = "com.bullhorn.orm.inmem.dao", entityManagerFactoryRef = "inMemEntityManager", transactionManagerRef = "inMemTransactionManager")
public class InMemDBConfig {

    private Environment env;

    @Autowired
    public void setEnv(Environment env) {
        this.env = env;
    }

    @Bean
    public LocalContainerEntityManagerFactoryBean inMemEntityManager() {
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setDataSource(inMemDataSource());
        em.setPackagesToScan(new String[]{"com.bullhorn.orm.inmem.model"});
        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        em.setJpaVendorAdapter(vendorAdapter);
        HashMap<String, Object> properties = new HashMap<>();
        properties.put("hibernate.hbm2ddl.auto", "create-drop");
        properties.put("hibernate.dialect", env.getProperty("org.hibernate.dialect.HSQLDialect"));

        em.setJpaPropertyMap(properties);
        return em;
    }

    @Bean
    public DataSource inMemDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.hsqldb.jdbc.JDBCDriver");
        dataSource.setUrl("jdbc:hsqldb:mem:azureConsumerDB");
        dataSource.setUsername("sa");
        dataSource.setPassword("nothing");
        return dataSource;
    }

    @Bean
    public PlatformTransactionManager inMemTransactionManager() {
        JpaTransactionManager transactionManager = new JpaTransactionManager();
        transactionManager.setEntityManagerFactory(inMemEntityManager().getObject());
        return transactionManager;
    }

    @Primary
    @Bean(name = "inMemJdbcTemplate")
    public JdbcTemplate inMemJdbcTemplate() {
        return new JdbcTemplate(inMemDataSource());
    }

    @Primary
    @Bean(name = "inMemNamedJdbcTemplate")
    public NamedParameterJdbcTemplate metricsNamedJdbcTemplate() {
        return new NamedParameterJdbcTemplate(inMemDataSource());
    }
}
