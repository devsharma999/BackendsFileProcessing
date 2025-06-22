package com.intern.batch.entity;

import org.junit.jupiter.api.Test;

import com.intern.batch.entity.FileLoad;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

class FileLoadTest {

    @Test
    void testNoArgsConstructorAndSetters() {
        FileLoad fileLoad = new FileLoad();
        fileLoad.setId(1L);
        fileLoad.setFilename("example.csv");
        fileLoad.setLoadDate(LocalDateTime.of(2025, 5, 19, 10, 30));
        fileLoad.setStatus("SUCCESS");
        fileLoad.setRecordCount(100);
        fileLoad.setErrors("");

        assertEquals(1L, fileLoad.getId());
        assertEquals("example.csv", fileLoad.getFilename());
        assertEquals(LocalDateTime.of(2025, 5, 19, 10, 30), fileLoad.getLoadDate());
        assertEquals("SUCCESS", fileLoad.getStatus());
        assertEquals(100, fileLoad.getRecordCount());
        assertEquals("", fileLoad.getErrors());
    }

    @Test
    void testAllArgsConstructor() {
        LocalDateTime now = LocalDateTime.now();
        FileLoad fileLoad = new FileLoad(1L, "data.csv", now, "FAIL", 50, "File corrupted");

        assertEquals(1L, fileLoad.getId());
        assertEquals("data.csv", fileLoad.getFilename());
        assertEquals(now, fileLoad.getLoadDate());
        assertEquals("FAIL", fileLoad.getStatus());
        assertEquals(50, fileLoad.getRecordCount());
        assertEquals("File corrupted", fileLoad.getErrors());
    }

    @Test
    void testToString() {
        LocalDateTime now = LocalDateTime.of(2025, 5, 19, 12, 0);
        FileLoad fileLoad = new FileLoad(5L, "log.csv", now, "SUCCESS", 20, "");

        String expected = "FileLoad [id=5, filename=log.csv, loadDate=" + now + ", status=SUCCESS, recordCount=20, errors=]";
        assertEquals(expected, fileLoad.toString());
    }

    @Test
    void testConstructorWithoutId() {
        LocalDateTime now = LocalDateTime.of(2025, 5, 19, 14, 0);
        FileLoad fileLoad = new FileLoad("new.csv", now, "SUCCESS", 77, "");

        assertEquals("new.csv", fileLoad.getFilename());
        assertEquals(now, fileLoad.getLoadDate());
        assertEquals("SUCCESS", fileLoad.getStatus());
        assertEquals(77, fileLoad.getRecordCount());
        assertEquals("", fileLoad.getErrors());
    }
}
