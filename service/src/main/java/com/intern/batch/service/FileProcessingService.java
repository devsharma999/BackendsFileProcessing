package com.intern.batch.service;
 
import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.transaction.Transactional;
import org.springframework.transaction.annotation.Propagation;
 
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;

import com.intern.batch.entity.FileLoad;
import com.intern.batch.entity.FileMetadata;
import com.intern.batch.exception.EmptyFieldException;
import com.intern.batch.exception.FileAlreadyProcessedException;
import com.intern.batch.exception.FileNameConflictException;
import com.intern.batch.exception.InvalidFilenameException;
import com.intern.batch.exception.InvalidHeaderException;
import com.intern.batch.exception.UnsupportedFileFormatException;
import com.intern.batch.repository.FileMetadataRepository;
import com.intern.batch.repository.FileRepository;
 
@Service
@Transactional
public class FileProcessingService {
 
    @PersistenceContext
    private EntityManager entityManager;
 
    @Autowired
    private FileMetadataRepository metadataRepo;
 
    @Autowired
    private FileRepository loadRepo;
    
    @Value("${batch.success.directory}")
	public String successDirectory;
 
    @Value("${batch.failed.directory}")
    public String failedDirectory;
    
    @Value("${batch.input.directory}")
    public String inputDirectory;
   
    static int total=0;
    
    public void processFile(File file) throws Exception {
       	String hit_errors="";
       	boolean isSuccessful=false;
       	
       	try {
    	 String name=file.getName();
    	 //name=name.substring(0,name.indexOf('.'));
    	 System.out.println(name);
//    	 String filename=file.getName().toLowerCase();
//    	 System.out.println(filename);
    	 if (!name.endsWith(".csv")) {
    		 hit_errors="Unsupported file format";
         	FileLoad obj=new FileLoad(name,LocalDateTime.now(),"FAILED",-1,hit_errors);
         	loadRepo.save(obj);
    	    throw new UnsupportedFileFormatException("Unsupported file format: " + name);
    	  	  
    	 }
    	 
    	 String regex = "^BSE_EQ_TM_TRADE_\\d{4}_\\d{8}_F\\.csv$";
    	    if (!name.matches(regex)) {
    	    	hit_errors="Invalid File Name";
            	FileLoad obj=new FileLoad(name,LocalDateTime.now(),"FAILED",-1,hit_errors);
            	loadRepo.save(obj);
    	        throw new InvalidFilenameException("Invalid filename format: " + name);
    	    }
    	
    	if (loadRepo.existsByFilename(file.getName())) {
    		//upsert("trade_records, rows,);
            throw new FileAlreadyProcessedException("File already processed: " + file.getName());
        }
    	
//    	if (loadRepo.existsByFilenameAndStatus(file.getName(), "SUCCESS")) {
//    	    hit_errors = "Duplicate filename";
//    	    logDuplicateFile(file.getName(), hit_errors);  // logs in separate transaction
//    	    throw new FileAlreadyProcessedException("File already processed: " + file.getName());
//    	}


    	
    	

    	
    	String hasher = computeFileHash(file);
    	if (metadataRepo.existsByFileHash(hasher)) {
    	    FileLoad load = new FileLoad();
    	    load.setFilename(file.getName());
    	    load.setErrors("Exactly same records");
    	    load.setRecordCount(-1);
    	    load.setLoadDate(LocalDateTime.now());
    	    load.setStatus("FAILED");
    	    loadRepo.save(load);

    	    throw new FileAlreadyProcessedException("Exactly same records: " + file.getName());
    	}

 
         
 
        String hash = computeFileHash(file);
        if (metadataRepo.existsByFileHash(hash)) {
            return;
        	//throw new FileAlreadyProcessedException("File already processed: " + file.getName());
        }
        
        String filename = file.getName();
        String fileHash = computeFileHash(file); // Implement SHA-256 or MD5 logic

        

        // Check if same filename was already processed successfully
        if (loadRepo.existsByFilenameAndStatus(filename, "SUCCESS")) {
            Optional<FileMetadata> existingMetaOpt = metadataRepo.findByFilename(filename);

            if (existingMetaOpt.isPresent()) {
                String existingHash = existingMetaOpt.get().getFileHash();

                if (!existingHash.equals(fileHash)) {
                    // Same filename, different content —> Conflict
                    String newFilename = generateUniqueFilename(filename);
                    String error = "Filename conflict: same name but different content";
                    System.out.println("New="+newFilename);
                    FileLoad conflictLog = new FileLoad(newFilename, LocalDateTime.now(), "FAILED", -1, error);
                    loadRepo.save(conflictLog);

                    throw new FileNameConflictException(error + " -> Renamed to: " + newFilename);
                } else {
                    // Same filename, same content —> Ignore
                    throw new FileAlreadyProcessedException("File already processed: " + filename);
                }
            }
        }

 
 
        List<String[]> rows = parseFile(file);
        if (rows.isEmpty()) return;
        
//        String[] headers = rows.get(0);
//        List<String> dataRows = rows.stream()
//            .skip(1)
//            .map(row -> String.join(",", row))
//            .toList();
        
       
        
        for (int i = 1; i < rows.size(); i++) // skip header
        { 
            String[] row = rows.get(i);
            for (int j = 0; j < row.length; j++) {
                if (row[j] == null || row[j].trim().isEmpty()) {
                	hit_errors="Null Values";
                	FileLoad obj=new FileLoad(file.getName(),LocalDateTime.now(),"FAILED",-1,hit_errors);
                	loadRepo.save(obj);
                    throw new EmptyFieldException("Empty field found in file '" + file.getName() +
                            "' at line " + (i + 1) + ", column " + (j + 1));
                }
            }
        }
 
        String tableName = getOrCreateBaseTable(file.getName(), rows.get(0),hit_errors);
//        upsert(tableName, headers, dataRows);
//        int totalInserted = dataRows.size();
        
//        int duplicateCount = 0;
//    	int insertedCount = 0;
//
//    	for (String[] row : rows.subList(1, rows.size())) { // skip header
//    	    if (isDuplicateRow(tableName, rows.get(0), row)) {
//    	        duplicateCount++;
//    	        continue;
//    	    }
//
//    	    // perform upsert() or insert logic
//    	    upsert(tableName, rows.get(0), Collections.singletonList(String.join(",", row)));
//    	    insertedCount++;
//    	}
//    	if (duplicateCount == rows.size() - 1) {
//    	    FileLoad load = new FileLoad();
//    	    load.setFilename(file.getName());
//    	    load.setErrors("Exactly same records");
//    	    load.setRecordCount(0);
//    	    load.setLoadDate(LocalDateTime.now());
//    	    load.setStatus("FAILED");
//    	    loadRepo.save(load);
//
//    	    throw new FileAlreadyProcessedException("Exactly same records");
//    	}
//        
        int inserted = insertDataIntoTable(tableName, rows,file);
        total+=inserted;
 
        FileMetadata meta = new FileMetadata();
        meta.setFilename(file.getName());
        meta.setTableName(tableName);
        meta.setFileHash(hash);
        metadataRepo.save(meta);
        
//        FileLoad load = new FileLoad();
//        load.setFilename(file.getName());
//        load.setErrors(hit_errors);
//        load.setRecordCount(inserted);
//        load.setLoadDate(LocalDateTime.now());
//        load.setStatus(hit_errors.isEmpty() ? "SUCCESS" : "FAILED");
//        loadRepo.save(load);
//
//        FileLoad load1 = new FileLoad();
//        load1.setFilename(tableName);
//        load1.setErrors("");
//        load1.setLoadDate(LocalDateTime.now());
//        load1.setStatus("SUCCESS");
//        load1.setRecordCount(total);
//        loadRepo.save(load1);
   //     isSuccessful = hit_errors.isEmpty();


        FileLoad load = new FileLoad();
        load.setFilename(file.getName());
        load.setErrors(hit_errors);
        load.setRecordCount(inserted);
        load.setLoadDate(LocalDateTime.now());
        
        if(hit_errors=="")
        	load.setStatus("SUCCESS");
        else
        	load.setStatus("FAILED");
        
        loadRepo.save(load);
        System.out.println(file.getName());
        
        isSuccessful = hit_errors.isEmpty();

       	}
       	
//       	catch (Exception e) {
//            FileLoad load = new FileLoad();
//            load.setFilename(file.getName());
//            load.setErrors(e.getMessage());
//            load.setRecordCount(-1);
//            load.setLoadDate(LocalDateTime.now());
//            load.setStatus("FAILED");
//            loadRepo.save(load);
//            
//            
//            throw e;
//        } 
       	
       	finally {
            moveFileAfterProcessing(file, isSuccessful);
        }

        }
    
    public String generateUniqueFilename(String baseName) {
        int count = 1;
        String nameWithoutExt = baseName;
        String ext = "";

        if (baseName.contains(".")) {
            int lastDot = baseName.lastIndexOf('.');
            nameWithoutExt = baseName.substring(0, lastDot);
            ext = baseName.substring(lastDot);
        }

        String candidate = baseName;
        while (loadRepo.existsByFilenameAndStatus(candidate, "SUCCESS")) {
            candidate = nameWithoutExt + "(" + count + ")" + ext;
            count++;
        }

        return candidate;
    }

    
//    public boolean isDuplicateRow(String tableName, String[] headers, String[] row) {
//        StringBuilder query = new StringBuilder("SELECT COUNT(*) FROM `" + tableName + "` WHERE ");
//        
//        for (int i = 0; i < headers.length; i++) {
//            query.append("`").append(headers[i]).append("` = :param").append(i);
//            if (i < headers.length - 1) query.append(" AND ");
//        }
//
//
//        Query nativeQuery = entityManager.createNativeQuery(query.toString());
//
//        for (int i = 0; i < headers.length; i++) {
//            nativeQuery.setParameter("param" + i, row[i]);
//        }
//
//        Number count = (Number) nativeQuery.getSingleResult();
//        return count.intValue() > 0;
//    }

    
    private void moveFileAfterProcessing(File file, boolean isSuccess) {
        File targetDir = new File(isSuccess ? successDirectory : failedDirectory);
        if (!targetDir.exists()) targetDir.mkdirs();
 
        File targetFile = new File(targetDir, file.getName());
 
        try {
            Files.move(file.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            System.err.println("Failed to move file: " + file.getAbsolutePath() + " to " + targetFile.getAbsolutePath());
            e.printStackTrace();
        }
    }
 
    private String getOrCreateBaseTable(String filename, String[] headers,String hit_errors) {
        List<String> tables = metadataRepo.findFirstTableName(PageRequest.of(0, 1));
        String tableName = "trade_records";
 
        if (!tableExists(tableName)) 
        {
            createTable(tableName, headers);
        } 
        else if (!headersMatch(tableName, headers,filename,hit_errors))
        {
            tableName = tableName + "_" + System.currentTimeMillis();
            createTable(tableName, headers);
        }
 
        return tableName;
    }
 
    @SuppressWarnings("unchecked")
    private boolean headersMatch(String tableName, String[] headers,String filename,String hit_errors) {
        List<Object[]> result = entityManager.createNativeQuery("DESCRIBE `" + tableName + "`").getResultList();
        List<String> dbCols = result.stream()
                .map(row -> row[0].toString())
                .filter(col -> !col.equalsIgnoreCase("sno"))
                .collect(Collectors.toList());
 
        List<String> fileCols = Arrays.stream(headers)
                .map(h -> h.trim().replaceAll("[^a-zA-Z0-9_]", "_"))
                .filter(h -> !h.equalsIgnoreCase("id"))
                .collect(Collectors.toList());
        boolean set=true;
        if (!dbCols.equals(fileCols)) {
        	hit_errors="Header mismatch";
        	FileLoad obj=new FileLoad(filename,LocalDateTime.now(),"FAILED",-1,hit_errors);
        	loadRepo.save(obj);
        	set=false;
            throw new InvalidHeaderException("Header mismatch for table '" + tableName + "'. Expected: " + dbCols + ", Found: " + fileCols);
        }
 
        return set;
    }
 
 
    private boolean tableExists(String tableName) {
        String db = entityManager.createNativeQuery("SELECT DATABASE()").getSingleResult().toString();
        BigInteger count = (BigInteger) entityManager.createNativeQuery(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?")
                .setParameter(1, db)
                .setParameter(2, tableName)
                .getSingleResult();
        return count.intValue() > 0;
    }
 
    private void createTable(String tableName, String[] headers) {
    	StringBuilder sb = new StringBuilder("CREATE TABLE `" + tableName + "` (");
    

    	List<String> columnDefs = new ArrayList<>();
    	for (String h : headers) {
    	    String col = sanitizeName(h);
    	    if (col.equalsIgnoreCase("trade_id")) {
    	        columnDefs.add("`" + col + "` VARCHAR(255) UNIQUE");  // 🔥 Make Trade_ID unique
    	    } else {
    	        columnDefs.add("`" + col + "` TEXT");
    	    }
    	}

    	sb.append(String.join(", ", columnDefs));
    	sb.append(")");

    	System.out.println("Creating table SQL: " + sb.toString());
    	entityManager.createNativeQuery(sb.toString()).executeUpdate();

    }


 
    private int insertDataIntoTable(String tableName, List<String[]> rows, File file) {
        if (rows.isEmpty()) return 0;

        String[] originalHeaders = rows.get(0);
        System.out.println("Headers from file: " + Arrays.toString(originalHeaders));
        
        String[] headers = Arrays.stream(originalHeaders)
        	    .map(h -> h.trim().toLowerCase().replace(" ", "_").replace("/", "_"))
        	    .toArray(String[]::new);

        int count = 0;
        int control=0; //variable to start headers check from 2nd file input
        
        //keeping headers in lowercase
        Map<String, Integer> headerMap = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
        	String normalized = headers[i].trim().toLowerCase().replace(" ", "_").replace("/", "_");
            headerMap.put(normalized, i);        }
        
       // private boolean headersMatch(String tableName, String[] headers,String filename,String hit_errors) {
        if(control==1)	
        { 
        	boolean exs=headersMatch(tableName,headers,file.getName(),"");
        }
        control=1;
        	//System.out.println("exs="+exs);
        // Ensure 'trade_id' exists in the headers (case-insensitive)
        if (!headerMap.containsKey("trade_id")) {
            throw new IllegalArgumentException("Missing 'trade_id' column.");
        }
        int repeat=0;
        for (int i = 1; i < rows.size(); i++) {
            String[] row = rows.get(i);
            String tradeId = row[headerMap.get("trade_id")];

            boolean exists = isTradeIdExists(tableName, tradeId);

            if (exists) {
                // Call upsert if trade_id already exists
            	repeat++;
                upsert(tableName, headers, List.of(row));
            } else {
                // Otherwise insert normally
                StringBuilder q = new StringBuilder("INSERT INTO `" + tableName + "` (");
                q.append(Arrays.stream(headers)
                               .map(String::trim)
                               .map(this::sanitizeName)
                               .map(h -> "`" + h + "`")
                               .collect(Collectors.joining(", ")));
                q.append(") VALUES (");
               q.append(Arrays.stream(row)
                               .map(val -> "'" + val.replace("'", "''") + "'")
                               .collect(Collectors.joining(", ")));
                q.append(")");
                entityManager.createNativeQuery(q.toString()).executeUpdate();
           }
            count++;
        }

        return count-repeat;
    }
   
   
    private boolean isTradeIdExists(String tableName, String tradeId) {
        String query = "SELECT COUNT(*) FROM `" + tableName + "` WHERE `trade_id` = :tradeId";
        Number count = (Number) entityManager.createNativeQuery(query)
            .setParameter("tradeId", tradeId)
            .getSingleResult();
        return count.intValue() > 0;
    }

 
    private List<String[]> parseFile(File file) throws IOException {
        List<String[]> rows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            boolean isFirst = true;
            while ((line = br.readLine()) != null) {
                String[] columns = line.split(",");
                if (isFirst) {
                    // Remove "id" from header row
                    columns = Arrays.stream(columns)
                            .filter(col -> !col.trim().equalsIgnoreCase("id"))
                            .toArray(String[]::new);
                    isFirst = false;
                }
                rows.add(columns);
            }
        }
        return rows;
    }
 
 
    private String computeFileHash(File file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream fis = new FileInputStream(file)) {
            byte[] byteArray = new byte[1024];
            int bytesCount;
            while ((bytesCount = fis.read(byteArray)) != -1) {
                digest.update(byteArray, 0, bytesCount);
            }
        }
        byte[] bytes = digest.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
 
    private String sanitizeName(String name) {
        return name.trim().replaceAll("[^a-zA-Z0-9_]", "_");
    }
    
    @Transactional
    public void upsert(String tableName, String[] headers, List<String> rows) {
        if (headers.length != rows.size()) {
            throw new IllegalArgumentException("Headers count and row values count do not match.");
        }

        StringBuilder sql = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        StringBuilder updateClause = new StringBuilder();

        // Sanitize and quote column names
        List<String> columns = Arrays.stream(headers)
            .map(this::sanitizeName)
            .collect(Collectors.toList());

        // Build INSERT INTO `table` (`col1`, `col2`, ...) VALUES (?, ?, ...)
        sql.append("INSERT INTO `").append(tableName).append("` (");
        sql.append(columns.stream().map(col -> "`" + col + "`").collect(Collectors.joining(", ")));
        sql.append(") VALUES (");
        placeholders.append(columns.stream().map(c -> "?").collect(Collectors.joining(", ")));
        sql.append(placeholders).append(")");

        // ON DUPLICATE KEY UPDATE `col1` = VALUES(`col1`), ...
        updateClause.append(" ON DUPLICATE KEY UPDATE ");
        updateClause.append(columns.stream()
            .filter(col -> !col.equalsIgnoreCase("trade_id")) // Skip updating the primary key
            .map(col -> "`" + col + "` = VALUES(`" + col + "`)")
            .collect(Collectors.joining(", "))
        );

        sql.append(updateClause);

        // Execute the query
        Query query = entityManager.createNativeQuery(sql.toString());
        for (int i = 0; i < rows.size(); i++) {
            query.setParameter(i + 1, rows.get(i));
        }
        query.executeUpdate();
    }
 
}