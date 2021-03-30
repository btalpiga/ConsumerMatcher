package v2.com.nyble;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class AppV2 {

    private static Logger logger = LoggerFactory.getLogger(AppV2.class);
    private static ConfigurableApplicationContext ctx;

    public static void main(String[] args) {
        ctx = SpringApplication.run(AppV2.class, args);
    }

    public static void closeApp(){
        if(ctx.isActive()){
            logger.warn("Closing Spring Boot Application");
            ctx.close();
        }
    }


}
