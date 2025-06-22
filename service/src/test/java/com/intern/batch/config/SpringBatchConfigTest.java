package com.intern.batch.config;

import com.intern.batch.config.SpringBatchConfig;
import com.intern.batch.entity.FileLoad;
import com.intern.batch.repository.FileRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.io.FileSystemResource;

import java.io.*;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SpringBatchConfigTest {

    @InjectMocks
    private SpringBatchConfig springBatchConfig;

    @Mock
    private FileRepository fileRepository;

    @Mock
    private StepContribution stepContribution;

    @Mock
    private ChunkContext chunkContext;

    @Captor
    private ArgumentCaptor<FileLoad> fileLoadCaptor;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testProcessCsvTasklet_successfulProcessing() throws Exception {
        // Prepare input file content
        String testFileName = "mock.csv";
        File tempFile = File.createTempFile("mock", ".csv");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
            writer.write("row1\nrow2\nrow3");
        }

        // Configure the input directory
        String inputDirectory = tempFile.getParent() + File.separator;
        springBatchConfig.inputDirectory = inputDirectory;

        // Copy file with specific name to simulate filename
        File renamed = new File(inputDirectory + testFileName);
        if (renamed.exists()) renamed.delete();
        tempFile.renameTo(renamed);

        // Set up FileRepository mock
        when(fileRepository.save(any(FileLoad.class))).thenAnswer(invocation -> {
            FileLoad f = invocation.getArgument(0);
            f.setId(1L);
            return f;
        });

        // Run tasklet
        RepeatStatus status = springBatchConfig.processCsvTasklet(testFileName)
            .execute(stepContribution, chunkContext);

        assertEquals(RepeatStatus.FINISHED, status);
        verify(fileRepository).save(fileLoadCaptor.capture());

        FileLoad saved = fileLoadCaptor.getValue();
        assertEquals(testFileName, saved.getFilename());
        assertEquals(3, saved.getRecordCount());
        assertEquals("SUCCESS", saved.getStatus());
        assertEquals("", saved.getErrors());
        assertNotNull(saved.getLoadDate());
    }

    @Test
    void testProcessCsvTasklet_withReadError_shouldSetFailStatus() throws Exception {
        String invalidFile = "nonexistent.csv";
        springBatchConfig.inputDirectory = "invalid/";

        when(fileRepository.save(any(FileLoad.class))).thenAnswer(invocation -> {
            FileLoad f = invocation.getArgument(0);
            f.setId(2L);
            return f;
        });

        RepeatStatus status = springBatchConfig.processCsvTasklet(invalidFile)
            .execute(stepContribution, chunkContext);

        assertEquals(RepeatStatus.FINISHED, status);
        verify(fileRepository).save(fileLoadCaptor.capture());

        FileLoad saved = fileLoadCaptor.getValue();
        assertEquals(invalidFile, saved.getFilename());
        assertEquals("FAIL", saved.getStatus());
        assertTrue(saved.getErrors() != null && !saved.getErrors().isEmpty());
    }
}
