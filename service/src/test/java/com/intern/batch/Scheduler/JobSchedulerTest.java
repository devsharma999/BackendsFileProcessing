package com.intern.batch.Scheduler;

import com.intern.batch.Scheduler.JobScheduler;
import com.intern.batch.service.FileProcessingService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;

import static org.mockito.Mockito.*;

public class JobSchedulerTest {

    @InjectMocks
    private JobScheduler jobScheduler;

    @Mock
    private FileProcessingService fileProcessingService;

    @TempDir
    Path tempDir;

    private File validCSV1;
    private File validCSV2;
    private File invalidFile;

    @BeforeEach
    public void setup() throws Exception {
        MockitoAnnotations.openMocks(this);

        // Set the inputDirectory to temporary folder
        jobScheduler.inputDirectory = tempDir.toString();

        // Create test CSV files
        validCSV1 = tempDir.resolve("file1.csv").toFile();
        try (FileWriter writer = new FileWriter(validCSV1)) {
            writer.write("col1,col2\nval1,val2");
        }

        validCSV2 = tempDir.resolve("file2.csv").toFile();
        try (FileWriter writer = new FileWriter(validCSV2)) {
            writer.write("col1,col2\nval3,val4");
        }

        // This file should be ignored
        invalidFile = tempDir.resolve("file.txt").toFile();
        try (FileWriter writer = new FileWriter(invalidFile)) {
            writer.write("This is not a CSV.");
        }
    }

    @Test
    public void testProcessTradeFiles_WithValidCSVs() throws Exception {
        // Mock successful file processing
        doNothing().when(fileProcessingService).processFile(any(File.class));

        jobScheduler.processTradeFiles();

        // Only 2 .csv files should be processed
        verify(fileProcessingService, times(1)).processFile(validCSV1);
        verify(fileProcessingService, times(1)).processFile(validCSV2);
        //verify(fileProcessingService, never()).processFile(invalidFile);
    }

    @Test
    public void testProcessTradeFiles_ExceptionHandling() throws Exception {
        // Simulate an exception when processing the first file
        doThrow(new RuntimeException("Simulated exception")).when(fileProcessingService).processFile(validCSV1);
        doNothing().when(fileProcessingService).processFile(validCSV2);

        jobScheduler.processTradeFiles();

        // First file triggers exception, second is still processed
        verify(fileProcessingService).processFile(validCSV1);
        verify(fileProcessingService).processFile(validCSV2);
    }

    @Test
    public void testProcessTradeFiles_EmptyDirectory() throws Exception {
        // Point to an empty folder
        File emptyFolder = tempDir.resolve("empty").toFile();
        emptyFolder.mkdir();
        jobScheduler.inputDirectory = emptyFolder.getAbsolutePath();

        jobScheduler.processTradeFiles();

        // No file should be processed
        verify(fileProcessingService, never()).processFile(any(File.class));
    }

    @Test
    public void testProcessTradeFiles_NullFileArray() throws Exception {
        // Point to non-existent directory
        jobScheduler.inputDirectory = tempDir.resolve("nonexistent").toString();

        jobScheduler.processTradeFiles();

        // No file should be processed
        verify(fileProcessingService, never()).processFile(any(File.class));
    }
}
