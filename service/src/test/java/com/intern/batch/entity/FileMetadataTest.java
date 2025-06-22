package com.intern.batch.entity;

import com.intern.batch.entity.FileMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class FileMetadataTest {

    private FileMetadata metadata;
    private Date createdAt;

    @BeforeEach
    void setUp() {
        createdAt = new Date();
        metadata = new FileMetadata(1L, "sample.csv", "table_sample", "abcd1234", createdAt);
    }

    @Test
    void testConstructorAndGetters() {
        assertEquals(1L, metadata.getId());
        assertEquals("sample.csv", metadata.getFilename());
        assertEquals("table_sample", metadata.getTableName());
        assertEquals("abcd1234", metadata.getFileHash());
        assertEquals(createdAt, metadata.getCreatedAt());
    }

    @Test
    void testSetters() {
        FileMetadata m = new FileMetadata();
        m.setId(2L);
        m.setFilename("file.csv");
        m.setTableName("table_file");
        m.setFileHash("efgh5678");
        Date now = new Date();
        m.setCreatedAt(now);

        assertEquals(2L, m.getId());
        assertEquals("file.csv", m.getFilename());
        assertEquals("table_file", m.getTableName());
        assertEquals("efgh5678", m.getFileHash());
        assertEquals(now, m.getCreatedAt());
    }

    @Test
    void testDefaultCreatedAtIsNotNull() {
        FileMetadata m = new FileMetadata();
        assertNotNull(m.getCreatedAt(), "createdAt should be initialized by default");
    }

    @Test
    void testToStringContainsExpectedFields() {
        String result = metadata.toString();
        assertTrue(result.contains("sample.csv"));
        assertTrue(result.contains("table_sample"));
        assertTrue(result.contains("abcd1234"));
        assertTrue(result.contains("FileMetadata"));
    }
}
