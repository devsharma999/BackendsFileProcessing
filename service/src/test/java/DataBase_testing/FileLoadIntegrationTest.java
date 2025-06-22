package DataBase_testing;

import Common_Func.Common_Function;
import baseDB.BaseDB;

import java.io.File;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.Test;

public class FileLoadIntegrationTest extends BaseDB {

	@Test(priority = 1)
	public void insertCSVFile() throws InterruptedException {

		String filename = "BSE_EQ_TM_TRADE_5556_22052025_F.csv";
		String sourcePath = "C:/Users/dev.sharma/Desktop/Test_Data/" + filename;
		String destinationPath = "C:/Users/dev.sharma/Desktop/Uploads";

		try {
			// Use common function to move file
			Common_Function.moveFileToUpload(sourcePath, destinationPath);
			Reporter.log("File moved to: " + destinationPath);

			// Check if the file exists at the destination
			File destinationFile = new File(destinationPath);
			Assert.assertTrue(destinationFile.exists(), "File does not exist at destination!");
			Reporter.log("CSV file uploaded successfully to destination: " + filename);

			// Poll the database to check for the file's presence
			boolean fileUploaded = false;
			int maxRetries = 5; // Maximum number of retries
			int retryCount = 0;
			int waitTime = 3000; // Wait time in milliseconds between retries

			while (retryCount < maxRetries && !fileUploaded) {
				Thread.sleep(waitTime);
				retryCount++;

				// Query the database using existing connection
				String query = "SELECT * FROM file_load WHERE filename = '" + filename + "'";
				stmt = conn.createStatement();
				ResultSet resultSet = stmt.executeQuery(query);

				if (resultSet.next()) {
					String st = resultSet.getString("status");
					if ("SUCCESS".equals(st)) {
						fileUploaded = true;
						int id = resultSet.getInt("id");
						String errors = resultSet.getString("errors");
						String loaddate = resultSet.getString("loaddate");
						long record_count = resultSet.getLong("record_count");
						String status = resultSet.getString("status");
						Reporter.log("Data from database- id: " + id + " errors: " + errors + " filename: " + filename
								+ " loaddate: " + loaddate + " record_count: " + record_count + " status: " + status);
					}
				}
			}

			if (fileUploaded) {
				Reporter.log("Pass-> CSV file found in database: " + filename);
			} else {
				Assert.fail("Fail-> CSV file not found in database after upload.");
			}

		} catch (SQLException e) {
			Assert.fail("Database query failed: " + e.getMessage());
		}
	}

	@Test(priority = 2)
	public void insertTXTFile() throws InterruptedException {

		String filename = "BSE_EQ_TM_TRADE_5511_22052025_F.txt";
		String sourcePath = "C:/Users/dev.sharma/Desktop/Test_Data/" + filename;
		String destinationPath = "C:/Users/dev.sharma/Desktop/Uploads";

		try {
			// Use common function to move file
			Common_Function.moveFileToUpload(sourcePath, destinationPath);
			Reporter.log("File moved to: " + destinationPath);

			// Check if the file exists at the destination
			File destinationFile = new File(destinationPath);
			Assert.assertTrue(destinationFile.exists(), "File does not exist at destination!");
			Reporter.log("txt file uploaded successfully to destination: " + filename);

			// Poll the database to check for the file's presence
			boolean fileUploaded = false;
			int maxRetries = 5; // Maximum number of retries
			int retryCount = 0;
			int waitTime = 3000; // Wait time in milliseconds between retries

			while (retryCount < maxRetries && !fileUploaded) {
				Thread.sleep(waitTime);
				retryCount++;

				// Query the database using existing connection
				String query = "SELECT * FROM file_load WHERE filename = '" + filename + "'";
				stmt = conn.createStatement();
				ResultSet resultSet = stmt.executeQuery(query);

				if (resultSet.next()) {
					String error = resultSet.getString("errors");
					if (error == "Unsupported file format") {
						fileUploaded = true;
						int id = resultSet.getInt("id");
						String errors = resultSet.getString("errors");
						String loaddate = resultSet.getString("loaddate");
						long record_count = resultSet.getLong("record_count");
						String status = resultSet.getString("status");
						Reporter.log("Data from database- id: " + id + " errors: " + errors + " filename: " + filename
								+ " loaddate: " + loaddate + " record_count: " + record_count + " status: " + status);
					}
				}
			}

			if (!fileUploaded) {
				Reporter.log("Pass-> txt file not uploaded in database");
			} else {
				Assert.fail("Fail-> txt file uploaded in database");
			}

		} catch (SQLException e) {
			Assert.fail("Database query failed: " + e.getMessage());
		}
	}

	@Test(priority = 3)
	public void insertXLSXFile() throws InterruptedException {

		String filename = "BSE_EQ_TM_TRADE_5512_22052025_F.xlsx";
		String sourcePath = "C:/Users/dev.sharma/Desktop/Test_Data/" + filename;
		String destinationPath = "C:/Users/dev.sharma/Desktop/Uploads" + filename;

		try {
			// Use common function to move file
			Common_Function.moveFileToUpload(sourcePath, destinationPath);
			Reporter.log("File moved to: " + destinationPath);

			// Check if the file exists at the destination
			File destinationFile = new File(destinationPath);
			Assert.assertTrue(destinationFile.exists(), "File does not exist at destination!");
			Reporter.log("xlsx file uploaded successfully to destination: " + filename);

			// Poll the database to check for the file's presence
			boolean fileUploaded = false;
			int maxRetries = 5; // Maximum number of retries
			int retryCount = 0;
			int waitTime = 3000; // Wait time in milliseconds between retries

			while (retryCount < maxRetries && !fileUploaded) {
				Thread.sleep(waitTime);
				retryCount++;

				// Query the database using existing connection
				String query = "SELECT * FROM file_load WHERE filename = '" + filename + "'";
				stmt = conn.createStatement();
				ResultSet resultSet = stmt.executeQuery(query);

				if (resultSet.next()) {
					String error = resultSet.getString("errors");
					if (error == "Unsupported file format") {
						fileUploaded = true;
						int id = resultSet.getInt("id");
						String errors = resultSet.getString("errors");
						String loaddate = resultSet.getString("loaddate");
						long record_count = resultSet.getLong("record_count");
						String status = resultSet.getString("status");
						Reporter.log("Data from database- id: " + id + " errors: " + errors + " filename: " + filename
								+ " loaddate: " + loaddate + " record_count: " + record_count + " status: " + status);
					}
				}
			}

			if (!fileUploaded) {
				Reporter.log("Pass-> xlsx file not uploaded in database");
			} else {
				Assert.fail("Fail-> xlsx file uploaded in database");
			}

		} catch (SQLException e) {
			Assert.fail("Database query failed: " + e.getMessage());
		}
	}

	@Test(priority = 4)
	public void pngFile() throws InterruptedException { // not handled, processed but not uploaded in database
		String filename = "BSE_EQ_TM_TRADE_5513_22052025_F.png";

		String sourcePath = "C:/Users/dev.sharma/Desktop/Test_Data/" + filename;
		String destinationPath = "C:/Users/dev.sharma/Desktop/Uploads";

		try {
			// Use common function to move file
			Common_Function.moveFileToUpload(sourcePath, destinationPath);
			Reporter.log("File moved to: " + destinationPath);

			// Check if the file exists at the destination
			File destinationFile = new File(destinationPath);
			Assert.assertTrue(destinationFile.exists(), "File does not exist at destination!");
			Reporter.log("png file uploaded successfully: " + filename);

			// Poll the database to check for the file's presence
			boolean fileUploaded = false;
			int maxRetries = 5; // Maximum number of retries
			int retryCount = 0;
			int waitTime = 3000; // Wait time in milliseconds between retries

			while (retryCount < maxRetries && !fileUploaded) {
				Thread.sleep(waitTime);
				retryCount++;

				// Query the database using existing connection
				String query = "SELECT * FROM file_load WHERE filename = '" + filename + "'";
				stmt = conn.createStatement();
				ResultSet resultSet = stmt.executeQuery(query);

				if (resultSet.next()) {
					String error = resultSet.getString("errors");
					if (error == "Unsupported file format") {
						fileUploaded = true;
						int id = resultSet.getInt("id");
						String errors = resultSet.getString("errors");
						String loaddate = resultSet.getString("loaddate");
						long record_count = resultSet.getLong("record_count");
						String status = resultSet.getString("status");
						Reporter.log("Data from database- id: " + id + " errors: " + errors + " filename: " + filename
								+ " loaddate: " + loaddate + " record_count: " + record_count + " status: " + status);
					}
				}
			}

			if (!fileUploaded) {
				Reporter.log("Pass-> png File not uploaded in database");
			} else {
				Assert.fail("Fail-> png File uploaded to database");
			}

		} catch (SQLException e) {
			Assert.fail("Database connection or query failed: " + e.getMessage());
		}
	}

	@Test(priority = 5)
	public void jpgFile() throws InterruptedException { // not handled, processed but not uploaded in database
		String filename = "BSE_EQ_TM_TRADE_5514_22052025_F.jpg";

		String sourcePath = "C:/Users/dev.sharma/Desktop/Test_Data/" + filename;
		String destinationPath = "C:/Users/dev.sharma/Desktop/Uploads";

		try {
			// Use common function to move file
			Common_Function.moveFileToUpload(sourcePath, destinationPath);
			Reporter.log("File moved to: " + destinationPath);

			// Check if the file exists at the destination
			File destinationFile = new File(destinationPath);
			Assert.assertTrue(destinationFile.exists(), "File does not exist at destination!");
			Reporter.log("jpg file uploaded successfully: " + filename);

			// Poll the database to check for the file's presence
			boolean fileUploaded = false;
			int maxRetries = 5; // Maximum number of retries
			int retryCount = 0;
			int waitTime = 3000; // Wait time in milliseconds between retries

			while (retryCount < maxRetries && !fileUploaded) {
				Thread.sleep(waitTime);
				retryCount++;

				// Query the database using existing connection
				String query = "SELECT * FROM file_load WHERE filename = '" + filename + "'";
				stmt = conn.createStatement();
				ResultSet resultSet = stmt.executeQuery(query);

				if (resultSet.next()) {
					String error = resultSet.getString("errors");
					if (error == "Unsupported file format") {
						fileUploaded = true;
						int id = resultSet.getInt("id");
						String errors = resultSet.getString("errors");
						String loaddate = resultSet.getString("loaddate");
						long record_count = resultSet.getLong("record_count");
						String status = resultSet.getString("status");
						Reporter.log("Data from database- id: " + id + " errors: " + errors + " filename: " + filename
								+ " loaddate: " + loaddate + " record_count: " + record_count + " status: " + status);
					}
				}
			}

			if (!fileUploaded) {
				Reporter.log("Pass-> jpg File not uploaded in database");
			} else {
				Assert.fail("Fail-> jpg File uploaded to database");
			}

		} catch (SQLException e) {
			Assert.fail("Database connection or query failed: " + e.getMessage());
		}
	}

	@Test(priority = 6)
	public void checkLoadDateIsCurrent() throws InterruptedException {

		String filename = "BSE_EQ_TM_TRADE_5555_22052025_F.csv";
		String sourcePath = "C:/Users/dev.sharma/Desktop/Test_Data/" + filename;
		String destinationPath = "C:/Users/dev.sharma/Desktop/Uploads";

		try {
			// Move file using common function
			Common_Function.moveFileToUpload(sourcePath, destinationPath);
			Reporter.log("File moved to upload folder: " + destinationPath);
			System.out.println("File moved to upload folder: " + destinationPath);

			// Check if the file exists at the destination
			File destinationFile = new File(destinationPath);
			Assert.assertTrue(destinationFile.exists(), "File does not exist at destination!");
			Reporter.log("File uploaded successfully to destination: " + filename);
			System.out.println("File exists at destination: " + filename);

			// Poll the database to check for the file's presence
			boolean fileUploaded = false;
			int maxRetries = 5; // Maximum number of retries
			int retryCount = 0;
			int waitTime = 3000; // Wait time in milliseconds between retries

			while (retryCount < maxRetries && !fileUploaded) {
				Thread.sleep(waitTime);
				retryCount++;

				// Query to fetch loaddate
				String query = "SELECT loaddate FROM file_load WHERE filename = ?";
				PreparedStatement pstmt = conn.prepareStatement(query);
				pstmt.setString(1, filename);
				ResultSet resultSet = pstmt.executeQuery();

				if (resultSet.next()) {
					fileUploaded = true;
					String loaddateStr = resultSet.getString("loaddate");
					DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
					LocalDateTime loaddate = LocalDateTime.parse(loaddateStr, formatter);
					LocalDateTime now = LocalDateTime.now();

					// Check if loaddate is within 5 minutes of current time
					boolean isCurrent = loaddate.isAfter(now.minusMinutes(5)) && loaddate.isBefore(now.plusMinutes(5));
					if (isCurrent) {
						Reporter.log("Pass-> Load date is current: " + loaddateStr);
						System.out.println("Load date is current: " + loaddateStr);
					} else {
						System.out.println("Load date is not current: " + loaddateStr);
						Assert.fail("Fail-> Load date is not current: " + loaddateStr);
					}
				}
			}

			if (!fileUploaded) {
				System.out.println("No record found for the specified filename.");
				Assert.fail("Fail-> No record found for the specified filename.");
			}

		} catch (SQLException e) {
			System.out.println("Database query failed: " + e.getMessage());
			Assert.fail("Database query failed: " + e.getMessage());
		} catch (InterruptedException e) {
			System.out.println("Thread sleep interrupted: " + e.getMessage());
			Assert.fail("Thread sleep interrupted: " + e.getMessage());
		}
	}

	@Test(priority = 7)
	public void emptyFile() throws InterruptedException {

		String filename = "BSE_EQ_TM_TRADE_5559_22052025_F.csv";
		String sourcePath = "C:/Users/dev.sharma/Desktop/Test_Data/" + filename;
		String destinationPath = "C:/Users/dev.sharma/Desktop/Uploads/";

		try {
			// Move file using common function
			Common_Function.moveFileToUpload(sourcePath, destinationPath);
			Reporter.log("File moved to: " + destinationPath);

			// Check if the file exists at the destination
			File destinationFile = new File(destinationPath);
			Assert.assertTrue(destinationFile.exists(), "File does not exist at destination!");
			Reporter.log("Empty file uploaded successfully to destination: " + filename);

			// Poll the database to check for the file's presence
			boolean fileUploaded = false;
			int maxRetries = 5; // Maximum number of retries
			int retryCount = 0;
			int waitTime = 3000; // Wait time in milliseconds between retries

			while (retryCount < maxRetries && !fileUploaded) {
				Thread.sleep(waitTime);
				retryCount++;

				// Query to check record count
				String query = "SELECT record_count FROM file_load WHERE filename = ?";
				PreparedStatement pstmt = conn.prepareStatement(query);
				pstmt.setString(1, filename);
				ResultSet resultSet = pstmt.executeQuery();

				if (resultSet.next()) {
					fileUploaded = true;
					long recordCount = resultSet.getLong("record_count");
					Assert.assertEquals(recordCount, 0, "Fail-> Record count should be zero for an empty file.");
					Reporter.log("Pass-> Record count is zero for the empty file.");
				}
			}

			if (!fileUploaded) {
				Assert.fail("Fail-> Record_count column not found");
			}

		} catch (SQLException e) {
			Assert.fail("Database query failed: " + e.getMessage());
		}
	}

	@Test(priority = 8)
	public void checkErrorsColumnAllowsNull() {

		try {
			ResultSet rs = conn.getMetaData().getColumns(null, null, "file_load", "errors");

			if (rs.next()) {
				int nullable = rs.getInt("NULLABLE"); // 0 = No, 1 = Yes, 2 = unknown
				boolean allowsNull = (nullable == DatabaseMetaData.columnNullable);
				Reporter.log("errors column allows NULL: " + allowsNull);
				Assert.assertTrue(allowsNull, "Fail-> The 'errors' column should allow NULL values.");
				Reporter.log("Pass-> The 'errors' column allows NULL values.");
			} else {
				Assert.fail("Column 'errors' not found in table 'file_load'.");
			}
		} catch (SQLException e) {
			Assert.fail("Database error: " + e.getMessage());
		}
	}

	@Test(priority = 9)
	public void checkRecordCountColumnType() {

		try {
			ResultSet rs = conn.getMetaData().getColumns(null, null, "file_load", "record_count");
			if (rs.next()) {
				String typeName = rs.getString("TYPE_NAME");
				Reporter.log("recordCount column type: " + typeName);
				Assert.assertTrue(typeName.equalsIgnoreCase("BIGINT"), "Fail-> recordCount should be BIGINT");
				Reporter.log("Pass-> recordCount is correctly set as BIGINT");
			} else {
				Assert.fail("record_count column not found");
			}
		} catch (SQLException e) {
			Assert.fail("Database error: " + e.getMessage());
		}
	}

	@Test(priority = 10)
	public void checkIdFileMetadata() {

		try {
			// Check if 'id' is primary key
			boolean isPrimaryKey = false;
			ResultSet pkRs = conn.getMetaData().getPrimaryKeys(null, null, "file_metadata");
			while (pkRs.next()) {
				String columnName = pkRs.getString("COLUMN_NAME");
				if ("id".equalsIgnoreCase(columnName)) {
					isPrimaryKey = true;
					break;
				}
			}

			// Check if 'id' is auto-increment
			boolean isAutoIncrement = false;
			ResultSet colRs = conn.getMetaData().getColumns(null, null, "file_metadata", "id");

			if (colRs.next()) {
				isAutoIncrement = "YES".equalsIgnoreCase(colRs.getString("IS_AUTOINCREMENT"));
			}

			Reporter.log("id is primary key: " + isPrimaryKey);
			Reporter.log("id is auto-increment: " + isAutoIncrement);

			Assert.assertTrue(isPrimaryKey, "Fail due to -> 'id' should be a primary key.");
			Assert.assertTrue(isAutoIncrement, "Fail due to -> 'id' should be auto-increment.");
			Reporter.log("Pass-> id is both primary key and auto-increment.");
		} catch (SQLException e) {
			Assert.fail("Database error: " + e.getMessage());
		}
	}

	
}