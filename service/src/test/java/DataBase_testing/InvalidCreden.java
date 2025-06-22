package DataBase_testing;

import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.SQLException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test; 

import baseDB.BaseDB;

public class InvalidCreden extends BaseDB {
    
    @DataProvider(name="invalidDbData")
    public Object[][] invalidData() {
        return new Object[][] {
            {"jdbc:mysql://localhost:3306/rrr","admin23","21431"}, // Invalid username and password
            {"jdbc:mysql://localhost:3306/rrr","","root@39"}, // Valid pass, empty username
            {"jdbc:mysql://localhost:3306/rrr","root",""}, // Valid username, empty password
            {"jdbc:mysql://localhost:3313/rrr","root","root@39"}, // Invalid port
            {"jdbc:mysql://localhost:3306/re","root","root@39"}, // Invalid database name
            {"","",""} // Empty credentials
        };
    }

    @Test(dataProvider = "invalidDbData")
    public void validateDatabaseConnection(String url, String username, String password) {
        Connection conn = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(url, username, password);
            
            if (conn != null) {
                System.err.println(" Connection SUCCESSFUL with invalid credentials: " + username + "/" + password);
                Assert.fail("Test failed: Invalid credentials SHOULD NOT establish a connection!");
            } else {
                System.out.println("Database rejected invalid credentials as expected!");
            }

        } catch (ClassNotFoundException e) {
            System.err.println(" MySQL Driver not found!");
            Assert.fail("MySQL Driver missing!");
        } catch (SQLException e) {
            System.out.println(" Database rejected invalid credentials as expected! Error: " + e.getMessage());
        } finally {
            // Ensure connection is closed after validation
            try {
                if (conn != null) conn.close();
            } catch (SQLException e) {
                System.err.println("Failed to close connection: " + e.getMessage());
            }
        }
    }

    @AfterClass
    public void tearDown() {
        System.out.println(" Test execution completed.");
    }
}
