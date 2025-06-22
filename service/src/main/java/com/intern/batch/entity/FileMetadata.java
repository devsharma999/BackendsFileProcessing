package com.intern.batch.entity;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

@Entity
@Table(name = "file_metadata")
public class FileMetadata {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String filename;

    @Column(name = "table_name")
    private String tableName;

    @Column(name = "file_hash", unique = true)
    private String fileHash;

    @Column(name = "created_at", updatable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date createdAt = new Date();
    
    public FileMetadata() {
		// TODO Auto-generated constructor stub
	}

	public FileMetadata(Long id, String filename, String tableName, String fileHash, Date createdAt) {
		super();
		this.id = id;
		this.filename = filename;
		this.tableName = tableName;
		this.fileHash = fileHash;
		this.createdAt = createdAt;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getFileHash() {
		return fileHash;
	}

	public void setFileHash(String fileHash) {
		this.fileHash = fileHash;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	@Override
	public String toString() {
		return "FileMetadata [id=" + id + ", filename=" + filename + ", tableName=" + tableName + ", fileHash="
				+ fileHash + ", createdAt=" + createdAt + "]";
	}
	
	

    
}
