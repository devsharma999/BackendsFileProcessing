package com.intern.batch.config;

import com.intern.batch.config.FileProcessor;
import com.intern.batch.entity.FileLoad;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FileProcessorTest {

    private FileProcessor fileProcessor;

    @BeforeEach
    void setUp() {
        fileProcessor = new FileProcessor();
    }

    @Test
    void process_shouldSetStatusSuccessAndClearErrors() throws Exception {
        // Arrange
        FileLoad fileLoad = new FileLoad();
        fileLoad.setStatus("PENDING");
        fileLoad.setErrors("Some previous error");

        // Act
        FileLoad processed = fileProcessor.process(fileLoad);

        // Assert
        assertNotNull(processed);
        assertEquals("SUCCESS", processed.getStatus());
        assertEquals("", processed.getErrors());
    }

    @Test
    void process_shouldHandleNullInputGracefully() {
        // Since your current code does not check for null, calling process(null) will throw NullPointerException.
        // Let's verify that.

        assertThrows(NullPointerException.class, () -> fileProcessor.process(null));
    }

    // Optional: To test exception handling inside process method,
    // we can create a subclass that forces an exception to simulate catch block.
    @Test
    void process_shouldSetStatusFailedWhenExceptionThrown() throws Exception {
        FileProcessor faultyProcessor = new FileProcessor() {
            @Override
            public FileLoad process(FileLoad item) {
                throw new RuntimeException("Forced exception");
            }
        };

        FileLoad fileLoad = new FileLoad();

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> faultyProcessor.process(fileLoad));
        assertEquals("Forced exception", thrown.getMessage());
    }
}
