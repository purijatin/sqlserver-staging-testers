
package com.kilo;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class SQLServerStagingDriver {

    private static final Logger LOG = Logger
            .getLogger(SQLServerStagingDriver.class);

    private static ApplicationContext applicationContext;

    public static void main(String[] args) {
        LOG.info("Starting context load");
        applicationContext = new ClassPathXmlApplicationContext(
                "classpath:com/kilo/applicationContext.xml");
        LOG.info("Inited");
        makeServiceCalls();
        System.exit(0);
    }

    private static void makeServiceCalls() {
        applicationContext.getBean("multiInsertStageDAO");
    }

}
