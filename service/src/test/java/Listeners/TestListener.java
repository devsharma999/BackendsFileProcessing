package Listeners;

import org.testng.ITestListener;
import org.testng.ITestResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestListener implements ITestListener {
    private static final Logger logger = LogManager.getLogger(TestListener.class);

    @Override
    public void onTestStart(ITestResult result) {
        logger.info("TEST STARTED: " + result.getName());
    }

    @Override
    public void onTestSuccess(ITestResult result) {
        logger.info("TEST PASSED: " + result.getName());
    }

    @Override
    public void onTestFailure(ITestResult result) {
        logger.error("TEST FAILED: " + result.getName());
    }

    @Override
    public void onTestSkipped(ITestResult result) {
        logger.warn("TEST SKIPPED: " + result.getName());
    }
}
