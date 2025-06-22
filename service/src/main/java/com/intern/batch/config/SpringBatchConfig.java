package com.intern.batch.config;

import com.intern.batch.entity.FileLoad;

import com.intern.batch.repository.FileRepository;

import org.springframework.batch.core.*;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;

import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;

import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;

import org.springframework.batch.core.launch.support.RunIdIncrementer;

import org.springframework.batch.core.step.tasklet.Tasklet;

import org.springframework.batch.repeat.RepeatStatus;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.beans.factory.annotation.Value;

import org.springframework.context.annotation.Bean;

import org.springframework.context.annotation.Configuration;

import org.springframework.core.io.FileSystemResource;

import org.springframework.batch.core.configuration.annotation.StepScope;

import java.io.BufferedReader;

import java.io.InputStreamReader;

import java.time.LocalDateTime;
 
@Configuration

@EnableBatchProcessing

public class SpringBatchConfig {
 
    @Autowired

    private JobBuilderFactory jobBuilderFactory;
 
    @Autowired

    private StepBuilderFactory stepBuilderFactory;
 
    @Autowired

    private FileRepository fileRepository;
 
    @Value("${batch.input.directory}")
	public String inputDirectory;
    
    @Bean

    public Job importJob(Step processCsvStep) {

        return jobBuilderFactory.get("metadataImportJob")

                .incrementer(new RunIdIncrementer())

                .start(processCsvStep)

                .build();

    }
 
    @Bean

    public Step processCsvStep(Tasklet processCsvTasklet) {

        return stepBuilderFactory.get("processCsvStep")

                .tasklet(processCsvTasklet)

                .build();

    }
 
    

    @Bean

    @StepScope

    public Tasklet processCsvTasklet(@Value("#{jobParameters['filename']}") String filename) {

        return (contribution, chunkContext) -> {

            String errors = "";

            int count = 0;
 
            System.out.println("JobParam filename: " + filename);
 
            try {

                FileSystemResource resource = new FileSystemResource(inputDirectory + filename);

                System.out.println(" Reading: " + resource.getFile().getAbsolutePath());

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {

                    while (reader.readLine() != null) 
                    	count++;

                }

            } catch (Exception e) {

                errors = e.getMessage();

            }
 
            FileLoad metadata = new FileLoad();

            metadata.setFilename(filename);

            metadata.setLoadDate(LocalDateTime.now());

            metadata.setRecordCount(count);
            
            FileProcessor ob=new FileProcessor();
            FileLoad ans=ob.process(metadata);
            metadata.setStatus(ans.getStatus());

            metadata.setErrors(errors);
            
            if(errors!="")
            	metadata.setStatus("FAIL");
            FileLoad saved = fileRepository.save(metadata);

            System.out.println(" Metadata saved with ID: " + saved.getId());
 
            return RepeatStatus.FINISHED;

        };

    }

}

 