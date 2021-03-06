package com.bullhorn.config.app;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@EnableSwagger2
@Configuration
public class SwaggerConfig {

	@Bean
	public Docket initDocket() {
		return new Docket(DocumentationType.SWAGGER_2).select()
				.apis(RequestHandlerSelectors.basePackage("com.bullhorn.rest")).paths(PathSelectors.any()).build()
				.apiInfo(metaInfo());
	}

	private ApiInfo metaInfo() {
		return new ApiInfo("Opera Azure Consumer API"
				, "This service takes care of getting the data from Azure Service Bus and then staging it into PNET DB. \n\n" +
				"Target Table: RefreshWork.dbo.tblIntegrationServiceBusMessages \n\n" +
				"NOTE: It is a multi threaded / multi instance application"
				, "1.0"
				, "Terms of service"
				, new Contact("Sachin Jain", "https://www.bullhorn.com", "sachin.jain@bullhorn.com")
				, "Apache License Version 2.0"
				, "https://www.apache.org/licenses/LICENSE-2.0");
	}

}
