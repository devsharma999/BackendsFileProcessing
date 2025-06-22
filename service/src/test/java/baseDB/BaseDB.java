package baseDB;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;

public class BaseDB {
    protected static Connection conn;
    protected static Statement stmt;
    protected static ResultSet rs;

    @BeforeClass
    public void setupDatabaseConnection() {
        // Set up database connection
        Properties props = new Properties();
        try {
            props.load(new FileInputStream("src/main/resources/application.properties"));

            String url = props.getProperty("spring.datasource.url");
            String user = props.getProperty("spring.datasource.username");
            String password = props.getProperty("spring.datasource.password");

            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(url, user, password);
            stmt = conn.createStatement();

            System.out.println("✅ Connected to MySQL successfully!");
            Assert.assertNotNull(conn, "❌ Database connection failed!");

        } catch (IOException e) {
            System.err.println("❌ Failed to read properties file: " + e.getMessage());
            Assert.fail("❌ Properties file missing!");
        } catch (ClassNotFoundException | SQLException e) {
            System.err.println("❌ Database connection error: " + e.getMessage());
            Assert.fail("❌ Failed to connect to database!");
        }
    }

    @AfterSuite
    public void tearDown() {
        try {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
            if (conn != null) conn.close();
            System.out.println("✅ Database connections closed successfully.");
        } catch (SQLException e) {
            System.err.println("❌ Error closing database resources: " + e.getMessage());
        }
    }
}
