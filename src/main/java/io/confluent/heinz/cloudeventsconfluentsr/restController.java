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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.heinz.JsonMsg;
import jakarta.servlet.http.HttpServletRequest; //Spring Boot 3 (and Spring Framework 6) require a baseline of Jakarte EE 10
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;

//import javax.servlet.http.HttpServletRequest; // No longer supported in Springboot 3

@org.springframework.web.bind.annotation.RestController
@RequestMapping(value = "/")
public class restController {
    @Autowired
    private Environment env;

    @Autowired
    private ApplicationContext applicationContext;

    ConfluentSession confluentSession;

    private final Log logger = LogFactory.getLog(restController.class);

    //ConfluentSession confluentSession = applicationContext.getBean(ConfluentSession.class);

    //Constructor
    @Autowired
    public restController(ApplicationContext applicationContext) {
        logger.info("+++++++++++++++++++++++++++ in REST constructor");
        try {
            if( this.confluentSession == null) {
                this.confluentSession = applicationContext.getBean(ConfluentSession.class);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PostMapping("/test")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void postMessage(@RequestBody JsonMsg request,
                            HttpServletRequest httpRequest) {

        String JsonStr = "";
        ObjectMapper mapper = new ObjectMapper();
        //Output the POST message to confirm what was received
        try {
            JsonStr = mapper.writeValueAsString(request);
        } catch (JsonProcessingException je) {
            logger.info("++++++++++++++++++++JSON Error: \n:");
            je.printStackTrace();
        }
        logger.info(String.format("JSON REST POST Data -> %s ", JsonStr));

        try {
            this.confluentSession.sendJsonMessageCE(request);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @RequestMapping("/logtest")
    public String index() {
        final Log logger = LogFactory.getLog(getClass());

        logger.trace("A TRACE Message");
        logger.debug("A DEBUG Message");
        logger.info("An INFO Message");
        logger.warn("A WARN Message");
        logger.error("An ERROR Message");

        return "See the Logs to check the output for the levels of supported logging ...";
    }
}