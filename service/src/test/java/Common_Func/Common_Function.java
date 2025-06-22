package Common_Func;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class Common_Function {

	public static boolean tableExists(Connection conn, String tableName) throws SQLException {
		String query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?";

		try (PreparedStatement stmt = conn.prepareStatement(query)) {
			stmt.setString(1, tableName);
			try (ResultSet rs = stmt.executeQuery()) {
				return rs.next() && rs.getInt(1) > 0; //
			}
		}
	}

	// USED IN TEST 4
	public static boolean moveFileToUpload(String sourcePath, String destinationPath) {
		File sourceFile = new File(sourcePath);
		System.out.println("Source File Exists? " + sourceFile.exists());
		System.out.println("Is Readable? " + sourceFile.canRead());
		System.out.println("Absolute Path: " + sourceFile.getAbsolutePath());

		File destinationFolder = new File(destinationPath);

		if (!destinationFolder.exists()) {
			destinationFolder.mkdirs(); // Ensure uploads directory exists
		}

		File targetFile = new File(destinationFolder, sourceFile.getName());

		try {
			Files.move(sourceFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
			System.out.println("File moved successfully: " + targetFile.getAbsolutePath());
			return true;
		} catch (IOException e) {
			System.out.println("Error moving file: " + e.getMessage());
			return false;
		}
	}

	// Fetch column data efficiently (UNCHANGED) USED IN TEST 4
	public static Map<String, String> fetchColumnData(Statement stmt, String fileName) throws SQLException {
		Map<String, String> data = new HashMap<>();
		ResultSet rs = stmt
				.executeQuery("SELECT id, loaddate, status, record_count, errors FROM file_load WHERE filename = '"
						+ fileName + "' ORDER BY loaddate DESC LIMIT 1");

		if (rs.next()) {
			for (String col : new String[] { "id", "loaddate", "status", "record_count", "errors" }) {
				data.put(col, rs.getString(col));
			}
		}
		// Close ResultSet to prevent leaks
		return data;
	}

	// Poll DB to confirm file upload (UNCHANGED) USED IN TEST 4
	public static boolean waitForFileUpload(Statement stmt, String fileName, int maxRetries, int retryInterval)
			throws SQLException, InterruptedException {
		for (int i = 0; i < maxRetries; i++) {
			System.out.println("Checking file upload: Attempt " + (i + 1));
			Thread.sleep(retryInterval);
			ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM file_load WHERE filename = '" + fileName + "'");
			if (rs.next() && rs.getInt(1) > 0) {
				rs.close();
				System.out.println(" File detected in database.");
				return true;
			}
			rs.close();
		}
		return false;
	}

	// Poll DB until column updates are detected (UNCHANGED) USED IN TEST 4
	public static boolean waitForColumnUpdate(Statement stmt, String fileName, int maxRetries, int retryInterval)
			throws SQLException, InterruptedException {
		for (int i = 0; i < maxRetries; i++) {
			System.out.println("Checking column update: Attempt " + (i + 1));

			// Query for updated columns
			ResultSet rs = stmt.executeQuery("SELECT status FROM file_load WHERE filename = '" + fileName + "'");

			if (rs.next()) {
				String status = rs.getString("status"); // Assuming "status" changes when updated
				if (!"Pending".equalsIgnoreCase(status)) { // Adjust condition as needed
					rs.close();
					System.out.println(" Column update detected in database.");
					return true;
				}
			}
			rs.close();
			Thread.sleep(retryInterval); // Wait before retrying
		}
		return false;
	}

	public static boolean validateAutoIncrementAndUniqueness(Connection conn, String tableName) throws SQLException {
		Statement stmt = conn.createStatement();

		// Query now correctly retrieves duplicate IDs
		String query = "SELECT id FROM " + tableName + " GROUP BY id HAVING COUNT(id) > 1";
		ResultSet rs = stmt.executeQuery(query);

		boolean duplicatesFound = rs.next(); // If true, duplicates exist

		rs.close();
		stmt.close();

		return !duplicatesFound; // Returns true if IDs are unique
	}

	// Poll the DB to check if file is recorded in `file_load`
	public static boolean pollForFileInDB(Connection conn, String fileName, int maxRetries, int retryInterval)
			throws SQLException, InterruptedException {
		Statement stmt = conn.createStatement();
		for (int i = 0; i < maxRetries; i++) {
			ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM file_load WHERE filename = '" + fileName + "'");
			if (rs.next() && rs.getInt(1) > 0) {
				rs.close();
				stmt.close();
				return true;
			}

			Thread.sleep(retryInterval);
		}

		return false;
	}

	// Get file status from `file_load`
	public static String getFileStatus(Connection conn, String fileName) throws SQLException {
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT status FROM file_load WHERE filename = '" + fileName + "'");

		String status = "UNKNOWN";
		if (rs.next()) {
			status = rs.getString("status");
		}

		return status;
	}

//	
}