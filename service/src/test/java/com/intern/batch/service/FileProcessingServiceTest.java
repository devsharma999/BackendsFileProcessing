package com.intern.batch.service;
 
import com.intern.batch.service.FileProcessingService;
import com.intern.batch.entity.FileLoad;
import com.intern.batch.entity.FileMetadata;
import com.intern.batch.exception.*;
import com.intern.batch.repository.FileMetadataRepository;
import com.intern.batch.repository.FileRepository;
 
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.*;
 
import javax.persistence.EntityManager;
import javax.persistence.Query;
 
import java.io.File;
import java.io.FileWriter;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.*;
 
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
 
public class FileProcessingServiceTest {
 
    @InjectMocks
    private FileProcessingService fileProcessingService;
 
    @Mock
    private FileRepository fileRepository;
 
    @Mock
    private FileMetadataRepository fileMetadataRepository;
 
    @Mock
    private EntityManager entityManager;
 
    @Mock
    private Query mockQuery;
 
    @TempDir
    Path tempDir;
 
    @BeforeEach
    public void setup() throws Exception {
        MockitoAnnotations.openMocks(this);
 
        fileProcessingService.successDirectory = tempDir.resolve("success").toString();
        fileProcessingService.failedDirectory = tempDir.resolve("failed").toString();
        fileProcessingService.inputDirectory = tempDir.toString();
 
        // Ensure mockQuery returns itself for all setParameter invocations
        when(mockQuery.setParameter(anyString(), any())).thenReturn(mockQuery);
        when(mockQuery.setParameter(anyInt(), any())).thenReturn(mockQuery);
    }
 
    private File createTestCSV(String fileName, String content) throws Exception {
        File file = tempDir.resolve(fileName).toFile();
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(content);
        }
        return file;
    }
 
    @Test
    public void testProcessFile_Success() throws Exception {
        File file = createTestCSV("BSE_EQ_TM_TRADE_2024_20240520_F.csv",
                "Trade_ID,Price,Quantity\nT123,100,10");
 
        when(fileRepository.existsByFilename(anyString())).thenReturn(false);
        when(fileMetadataRepository.existsByFileHash(anyString())).thenReturn(false);
        when(fileMetadataRepository.findFirstTableName(any())).thenReturn(Collections.emptyList());
 
        when(entityManager.createNativeQuery(startsWith("SELECT DATABASE()"))).thenReturn(mockQuery);
        when(mockQuery.getSingleResult()).thenReturn("test_db");
 
        when(entityManager.createNativeQuery(startsWith("SELECT COUNT(*)"))).thenReturn(mockQuery);
        when(mockQuery.getSingleResult()).thenReturn(BigInteger.ZERO);
 
        when(entityManager.createNativeQuery(startsWith("DESCRIBE"))).thenReturn(mockQuery);
        when(mockQuery.getResultList()).thenReturn(new ArrayList<>());
 
        when(entityManager.createNativeQuery(startsWith("CREATE TABLE"))).thenReturn(mockQuery);
        when(entityManager.createNativeQuery(startsWith("INSERT"))).thenReturn(mockQuery);
        when(mockQuery.executeUpdate()).thenReturn(1);
 
        when(entityManager.createNativeQuery(startsWith("SELECT COUNT(*) FROM"))).thenReturn(mockQuery);
        when(mockQuery.getSingleResult()).thenReturn(BigInteger.ZERO);
 
        fileProcessingService.processFile(file);
 
        verify(fileMetadataRepository).save(any(FileMetadata.class));
        verify(fileRepository).save(any(FileLoad.class));
    }
 
    @Test
    public void testUnsupportedFileFormat() throws Exception {
        File file = createTestCSV("invalid_file.txt", "Trade_ID,Price,Quantity\nT123,100,10");
 
        UnsupportedFileFormatException ex = assertThrows(UnsupportedFileFormatException.class,
                () -> fileProcessingService.processFile(file));
 
        assertTrue(ex.getMessage().contains("Unsupported file format"));
    }
 
    @Test
    public void testInvalidFilename() throws Exception {
        File file = createTestCSV("BSE_INVALID_20240520.csv", "Trade_ID,Price,Quantity\nT123,100,10");
 
        InvalidFilenameException ex = assertThrows(InvalidFilenameException.class,
                () -> fileProcessingService.processFile(file));
 
        assertTrue(ex.getMessage().contains("Invalid filename format"));
    }
 
    @Test
    public void testFileAlreadyProcessed() throws Exception {
        File file = createTestCSV("BSE_EQ_TM_TRADE_2024_20240520_F.csv",
                "Trade_ID,Price,Quantity\nT123,100,10");
 
        when(fileRepository.existsByFilename(anyString())).thenReturn(true);
 
        FileAlreadyProcessedException ex = assertThrows(FileAlreadyProcessedException.class,
                () -> fileProcessingService.processFile(file));
 
        assertTrue(ex.getMessage().contains("File already processed"));
    }
 
    @Test
    public void testEmptyFieldInCSV() throws Exception {
        File file = createTestCSV("BSE_EQ_TM_TRADE_2024_20240520_F.csv",
                "Trade_ID,Price,Quantity\nT123,,10");
 
        when(fileRepository.existsByFilename(anyString())).thenReturn(false);
        when(fileMetadataRepository.existsByFileHash(anyString())).thenReturn(false);
 
        EmptyFieldException ex = assertThrows(EmptyFieldException.class,
                () -> fileProcessingService.processFile(file));
 
        assertTrue(ex.getMessage().contains("Empty field found"));
    }
 
    @Test
    public void testHeaderMismatch() throws Exception {
        File file = createTestCSV("BSE_EQ_TM_TRADE_2024_20240520_F.csv",
                "Trade_ID,WrongColumn,Quantity\nT123,100,10");
 
        when(fileRepository.existsByFilename(anyString())).thenReturn(false);
        when(fileMetadataRepository.existsByFileHash(anyString())).thenReturn(false);
        when(fileMetadataRepository.findFirstTableName(any())).thenReturn(List.of("trade_records"));
 
        when(entityManager.createNativeQuery(startsWith("SELECT DATABASE()"))).thenReturn(mockQuery);
        when(mockQuery.getSingleResult()).thenReturn("test_db");
 
        when(entityManager.createNativeQuery(startsWith("SELECT COUNT(*)"))).thenReturn(mockQuery);
        when(mockQuery.getSingleResult()).thenReturn(BigInteger.ONE);
 
        when(entityManager.createNativeQuery(startsWith("DESCRIBE"))).thenReturn(mockQuery);
        when(mockQuery.getResultList()).thenReturn(List.of(
                new Object[]{"Trade_ID"},
                new Object[]{"Price"},
                new Object[]{"Quantity"}
        ));
 
        InvalidHeaderException ex = assertThrows(InvalidHeaderException.class,
                () -> fileProcessingService.processFile(file));
 
        assertTrue(ex.getMessage().contains("Header mismatch"));
    }
}
 