package com.harvey.simple;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author harvey
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    
    @Test
    public void test() throws RejectTaskException {
        ThreaticExecutor threaticExecutor = new ThreaticExecutor(3, 5, 0, TimeUnit.SECONDS);
        
        threaticExecutor.execute(() -> {
            log.info("running");
            try { TimeUnit.SECONDS.sleep(3); } catch (Exception e) { e.printStackTrace(); }
            log.info("stopped");
        });
        
        threaticExecutor.execute(() -> {
            log.info("running");
            try { TimeUnit.SECONDS.sleep(3); } catch (Exception e) { e.printStackTrace(); }
            log.info("stopped");
        });
        
        threaticExecutor.execute(() -> {
            log.info("running");
            try { TimeUnit.SECONDS.sleep(3); } catch (Exception e) { e.printStackTrace(); }
            log.info("stopped");
        });
        
        threaticExecutor.execute(() -> {
            log.info("running");
            try { TimeUnit.SECONDS.sleep(3); } catch (Exception e) { e.printStackTrace(); }
            log.info("stopped");
        });
        
        threaticExecutor.execute(() -> {
            log.info("running");
            try { TimeUnit.SECONDS.sleep(3); } catch (Exception e) { e.printStackTrace(); }
            log.info("stopped");
        });
        
        threaticExecutor.execute(() -> {
            log.info("running");
            try { TimeUnit.SECONDS.sleep(3); } catch (Exception e) { e.printStackTrace(); }
            log.info("stopped");
        });
        
        threaticExecutor.execute(() -> {
            log.info("running");
            try { TimeUnit.SECONDS.sleep(3); } catch (Exception e) { e.printStackTrace(); }
            log.info("stopped");
        });
        
        threaticExecutor.execute(() -> {
            log.info("running");
            try { TimeUnit.SECONDS.sleep(3); } catch (Exception e) { e.printStackTrace(); }
            log.info("stopped");
        });
        
        try { System.in.read(); } catch (Exception e) { e.printStackTrace(); }
    }
}
