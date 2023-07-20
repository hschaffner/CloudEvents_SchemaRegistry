/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.heinz.cloudeventsconfluentsr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.DefaultManagedTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@SpringBootApplication
//get references to all Spring Services and Components
@ComponentScan({"io.confluent.heinz", "io.confluent.heinz.cloudeventsconfluentsr"})
public class CloudEventsConfluentSrApplication {

    private static final Log logger = LogFactory.getLog(CloudEventsConfluentSrApplication.class);

    private static ApplicationContext applicationContext;

    public static void main(String[] args) {
        logger.info("Starting CloudEventsConfluentSrApplication");

        applicationContext = SpringApplication.run(CloudEventsConfluentSrApplication.class, args);

        //Create Confluent Consumer -- Producer and Rest Controller are automatically started with Spring
        ConfluentConsumer confluentConsumer = new ConfluentConsumer(applicationContext.getEnvironment());

    }
}
