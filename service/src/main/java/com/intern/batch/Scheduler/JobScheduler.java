package com.intern.batch.Scheduler;

import java.io.File;
import java.time.LocalDateTime;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.intern.batch.entity.FileLoad;
import com.intern.batch.exception.UnsupportedFileFormatException;
import com.intern.batch.repository.FileRepository;
import com.intern.batch.service.FileProcessingService;

//import com.spring.batch.service.FileProcessingService;

@Component
public class JobScheduler {
	
	
	@Value("${batch.input.directory}")
    public String inputDirectory;
 
    //private static final String INPUT_DIR = "uploads/";

    @Autowired
    private FileProcessingService fileProcessingService;
    

    @Scheduled(fixedRate = 10000) // every minute
    public void processTradeFiles() {
        File dir = new File(inputDirectory);
        
        File[] files = dir.listFiles(); // Accepts all files in the directory

        if (files == null) return;
        
        for (File file : files) {
            try {
                fileProcessingService.processFile(file);
            } 
           
            catch (Exception e) {
                e.printStackTrace();
            }
            
        }
    }
}