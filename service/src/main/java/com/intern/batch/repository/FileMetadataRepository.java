package com.intern.batch.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.intern.batch.entity.FileMetadata;

public interface FileMetadataRepository extends JpaRepository<FileMetadata,Long> {
    boolean existsByFileHash(String hash);
   // int getRowCount(String tableName);
    long countByFileHash(String fileHash);
    Optional<FileMetadata> findByFilename(String filename);

    @Query("SELECT fm.tableName FROM FileMetadata fm ORDER BY fm.createdAt ASC")
    List<String> findFirstTableName(Pageable pageable);
   //FileMetadata findTopByOrderByCreatedAtAsc();
}

//public interface FileLoadRepository extends JpaRepository<FileLoad, Long> {}

