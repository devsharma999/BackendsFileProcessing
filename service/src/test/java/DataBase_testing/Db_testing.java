package DataBase_testing;

import Common_Func.Common_Function;


//import Extent_Manager.ExtentManager;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import baseDB.BaseDB;
import java.util.Arrays;
import java.util.Map;

import org.testng.annotations.Test;
import org.testng.Assert;
import org.testng.annotations.AfterClass;

public class Db_testing extends BaseDB {
	@Test(priority = 1)
	public void file_loadTableExistence() throws SQLException {

		if (Common_Function.tableExists(conn, "file_load")) {
			System.out.println(" Table Exists: file_load");
			Assert.assertTrue(true, "Table exists in the database!");
		} else {
			System.out.println(" Table Not Found: file_load");
			Assert.fail(" Table does not exist in the database!");
		}

	}

	@Test(priority = 2)
	public void columnsExistence() throws SQLException {
		
		String[] expectedColumns = { "id", "filename", "loaddate", "status", "record_count", "errors" };
		String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'file_load' AND TABLE_SCHEMA = 'rrr'";
		stmt = conn.createStatement();
		rs = stmt.executeQuery(sql);

		boolean allColumnsExist = true;

		while (rs.next()) {
			String columnName = rs.getString("COLUMN_NAME");
			boolean exists = Arrays.asList(expectedColumns).contains(columnName);

			if (!exists) {
				System.out.println(" Missing Column: " + columnName);
				
				allColumnsExist = false;
			} 
		}

		Assert.assertTrue(allColumnsExist, " Some columns are missing in file_load!");

	}

	@Test(priority = 4)
	public void existenceOfFile() throws SQLException {

		String str = "SELECT COUNT(*) AS TOTALFILES FROM file_load";
		ResultSet rs = stmt.executeQuery(str);

		if (rs.next() && rs.getInt("TOTALFILES") > 0) {
			System.out.println(" File Exists");
			
			Assert.assertTrue(true, "File exists in the database!");
		} else {
			System.out.println(" No File Found");
			
			Assert.fail("File not found in the database!");
		}

	}

	@Test(priority = 3)
	public void validateColumnUpdatesfile_load() throws SQLException, InterruptedException {
		
		String fileName = "BSE_EQ_TM_TRADE_5557_22052025_F.csv";
		int retryInterval = 3000;
		int maxRetries = 5;

		Assert.assertNotNull(conn, " Database connection is not initialized!");
		stmt = conn.createStatement();

		// Step 1: Fetch before upload data BEFORE file upload
		Map<String, String> beforeUploadData = Common_Function.fetchColumnData(stmt, fileName);
		

		// Step 2: Upload file
		boolean uploadSuccess = Common_Function.moveFileToUpload("C:/Users/dev.sharma/Desktop/Test_Data/"+fileName,"C:/Users/dev.sharma/Desktop/Uploads");
		System.out.println("Checking for file to get into the db");
		Assert.assertTrue(uploadSuccess, " File upload  failed!");

		// Remove explicit `commit()` as auto-commit is enabled
		conn.setAutoCommit(true); // Ensure auto-commit mode

		// Step 3: Ensure file upload is detected
		boolean fileUploaded = Common_Function.waitForFileUpload(stmt, fileName, maxRetries, retryInterval);
		Assert.assertTrue(fileUploaded, " File upload not detected in DB!");

		boolean columnUpdated = Common_Function.waitForColumnUpdate(stmt, fileName, maxRetries, retryInterval);
		Assert.assertTrue(columnUpdated, " Column update not detected in DB!");

		// Force fresh data retrieval before fetching `afterUploadData`
		stmt.execute("FLUSH TABLES");

		// Introduce a retry loop before fetching afterUploadData
		Map<String, String> afterUploadData = null;
		int retries = maxRetries;
		while (retries-- > 0) {
			Thread.sleep(retryInterval); // Wait between retries
			afterUploadData = Common_Function.fetchColumnData(stmt, fileName);

			if (!beforeUploadData.equals(afterUploadData)) {
				System.out.println(" Column data updated!");
				break; // Exit loop once changes are detected
			}
			System.out.println(" Waiting for column updates...");
		}

		

		// Step 6: Validate column updates
		boolean allColumnsUpdated = compareData(beforeUploadData, afterUploadData);

		if (allColumnsUpdated) {
			
		} else {
			
			Assert.fail(" Columns failed validation!");
		}
	}

	private boolean compareData(Map<String, String> beforeData, Map<String, String> afterData) {
		boolean allUpdated = true;

		for (String column : afterData.keySet()) { // Compare against afterData keys
			String beforeValue = beforeData.getOrDefault(column, "N/A");
			String afterValue = afterData.get(column);

			System.out
					.println(" Comparing Column: " + column + " | Before: " + beforeValue + " | After: " + afterValue);

			if (!beforeValue.equals(afterValue)) {
				
			} else {
				
				allUpdated = false;
			}
		}
		return allUpdated;
	}
	@Test(priority = 5)
	public void validateDuplicateContent() throws SQLException, InterruptedException {
	    String fileName = "BSE_EQ_TM_TRADE_5558_22052025_F.csv";

	    // Step 1: Upload the file
	    boolean uploadSuccess = Common_Function.moveFileToUpload("C:/Users/dev.sharma/Desktop/Test_Data/"+fileName, "C:/Users/dev.sharma/Desktop/Uploads");

	    // Step 2: Wait for the scheduler to process the file before validating
	    if (uploadSuccess) {
	        boolean recordFound = false;
	        int maxRetries = 5; 
	        int retryCount = 0;
	        int waitTime = 3000; 
	        while (retryCount < maxRetries && !recordFound) {
	            Thread.sleep(waitTime);
	            retryCount++;

	            String query = "SELECT errors FROM file_load WHERE filename = '" + fileName + "'";
	            try (Statement stmt = conn.createStatement();
	                 ResultSet resultSet = stmt.executeQuery(query)) {

	                if (resultSet.next()) {
	                    String errorText = resultSet.getString("errors");
	                    System.out.println("✅ Retrieved error message: " + errorText);

	                    if ("Exactly same records".equals(errorText)) {
	                        recordFound = true;
	                        break; // Exit loop once validation is successful
	                    }
	                }
	            }
	        }

	        if (!recordFound) {
	            Assert.fail("❌ Test failed: File record not found in database within expected time.");
	        }
	    } else {
	        Assert.fail("❌ Test failed: File upload did not succeed.");
	    }
	}


	@Test(priority = 6)
	public void testIDValidationForfile_loadTable() throws SQLException {
		

		// Validate IDs for file_load table
		boolean fileLoadValid = Common_Function.validateAutoIncrementAndUniqueness(conn, "file_load");
		if (fileLoadValid) {
			
		} else {
			
			Assert.fail(" ID validation failed for file_load.");
		}
	}

	@Test(priority = 7) // Table name not confirmed Yet...
	public void file_metadataTablexistence() throws SQLException {
		

		if (Common_Function.tableExists(conn, "file_metadata")) {
			
			System.out.println(" Table Exists: file_metadata");
			
			Assert.assertTrue(true, "Table exists in the database!");
		} else {
			
			System.out.println(" Table Not Found: customers");
			
			Assert.fail(" Table does not exist in the database!");
		}

	}

	@Test(priority = 8) // Can be change acc. to dev. LINE NO.287
	public void file_metadatacolumnExists() throws SQLException {
		
		String[] expectedColumns = { "id", "created_at", "file_hash", "filename", "table_name" };
		String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'file_metadata' AND TABLE_SCHEMA = 'rrr'";
		stmt = conn.createStatement();
		rs = stmt.executeQuery(sql);

		boolean allColumnsExist = true;

		while (rs.next()) {
			String columnName = rs.getString("COLUMN_NAME");
			boolean exists = Arrays.asList(expectedColumns).contains(columnName);

			if (!exists) {
				System.out.println(" Missing Column: " + columnName);
				
				allColumnsExist = false;
			} else {
				
			}
		}

		Assert.assertTrue(allColumnsExist, "Some columns are missing in customers!");
	}

	@Test(priority = 9)
	public void recordProcessed() throws SQLException {
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(
				"SELECT (SELECT COALESCE(SUM(record_count), 0) FROM file_load WHERE status != 'FAILED' AND record_count >= 0) AS fileLoadTotal, "
						+ "(SELECT COUNT(*) FROM trade_records) AS customersTotal");

		int fileLoadTotal = 0, customersTotal = 0;
		if (rs.next()) {
			fileLoadTotal = rs.getInt("fileLoadTotal"); // Only counts valid processed records
			customersTotal = rs.getInt("customersTotal"); // Actual processed records in customers table
		}

		System.out.println("Expected Records (Processed Files Only): " + fileLoadTotal + " | Processed Records in DB: "
				+ customersTotal);

		// Validation Logic: Compare actual vs. expected processed records
		if (fileLoadTotal == customersTotal) {
		} else {
			Assert.fail(" Record count validation failed.");
		}
	}

	@Test(priority = 10)
	public void UnstructuredFile() throws SQLException, InterruptedException {
		String fileName = "BSE_EQ_TM_TRADE_5510_22052025_F.csv";
		boolean uploaded = Common_Function.moveFileToUpload("C:/Users/dev.sharma/Desktop/Test_Data/"+fileName,"C:/Users/dev.sharma/Desktop/Uploads");
		if (uploaded)
			System.out.print("Uploaded");
		else
			System.out.print("Error in Path");

		// Step 2: Poll the `file_load` table to verify file presence
		boolean fileExistsInDB = Common_Function.pollForFileInDB(conn, fileName, 5, 5000);
		Assert.assertTrue(fileExistsInDB, " File not found in `file_load` table!");

		// Step 3: Verify status is "NOT PROCESSED"
		String fileStatus = Common_Function.getFileStatus(conn, fileName);
		Assert.assertEquals(fileStatus, "FAILED", " File should not be processed!");
		if (fileExistsInDB && "FAILED".equals(fileStatus)) {
		    Assert.assertTrue(true, "Test Passed: File exists in DB with status FAILED.");
		} else {
		    Assert.fail("Test Failed: Conditions not met.");
		}

	}
	

	@AfterClass
	public void cleanup() {
		try {

			if (stmt != null) {
				stmt.close();
				System.out.println(" Statement closed.");
			}

			if (conn != null) {
				conn.close();
				System.out.println(" Database connection closed.");
			}
		} catch (SQLException e) {
			System.err.println(" Error closing database resources: " + e.getMessage());
		}
	}

}
