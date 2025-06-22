package com.intern.batch.config;


import org.springframework.batch.item.ItemProcessor;

import com.intern.batch.entity.FileLoad;

public class FileProcessor implements ItemProcessor<FileLoad, FileLoad> {

    @Override
    public FileLoad process(FileLoad item) {
        try {
            item.setStatus("SUCCESS");
            item.setErrors(""); 
        } catch (Exception e) {
            item.setStatus("FAILED");
            item.setErrors(e.getMessage());
        }
        return item;
    }
}
