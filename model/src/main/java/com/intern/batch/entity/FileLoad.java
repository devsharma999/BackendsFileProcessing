package com.intern.batch.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "file_load") //FILE_LOAD
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FileLoad {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ID")
    private long id;
    
    @Column(nullable = false,name = "filename")
    private String filename;
   
    @Column(name = "loaddate")
    private LocalDateTime loadDate;
    
    @Column(name = "status")
    private String status;
   
    @Column(name = "recordCount")
    private long recordCount;
    
    @Column(name = "errors")
    private String errors;
    
    public FileLoad() {
		// TODO Auto-generated constructor stub
	}

	public FileLoad(long id, String filename, LocalDateTime loadDate, String status, long recordCount, String errors) {
		super();
		this.id = id;
		this.filename = filename;
		this.loadDate = loadDate;
		this.status = status;
		this.recordCount = recordCount;
		this.errors = errors;
	}
	public FileLoad( String filename, LocalDateTime loadDate, String status, long recordCount, String errors) {
		super();
		
		this.filename = filename;
		this.loadDate = loadDate;
		this.status = status;
		this.recordCount = recordCount;
		this.errors = errors;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public LocalDateTime getLoadDate() {
		return loadDate;
	}

	public void setLoadDate(LocalDateTime loadDate) {
		this.loadDate = loadDate;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public long getRecordCount() {
		return recordCount;
	}

	public void setRecordCount(long recordCount) {
		this.recordCount = recordCount;
	}

	public String getErrors() {
		return errors;
	}

	public void setErrors(String errors) {
		this.errors = errors;
	}

	@Override
	public String toString() {
		return "FileLoad [id=" + id + ", filename=" + filename + ", loadDate=" + loadDate + ", status=" + status
				+ ", recordCount=" + recordCount + ", errors=" + errors + "]";
	}
}
    
    