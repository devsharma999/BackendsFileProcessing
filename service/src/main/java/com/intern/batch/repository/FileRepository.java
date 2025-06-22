package com.intern.batch.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.intern.batch.entity.FileLoad;

public interface FileRepository  extends JpaRepository<FileLoad,Integer> {
	public boolean existsByFilename(String filename);
	public boolean existsByFilenameAndStatus(String filename, String status);


}
