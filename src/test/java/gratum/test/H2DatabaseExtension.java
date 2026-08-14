package gratum.test;

import groovy.sql.Sql;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class H2DatabaseExtension implements BeforeEachCallback, AfterEachCallback {

    private static final String JDBC_URL = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
    private static final String USER = "sa";
    private static final String PASSWORD = "";

    private Connection connection;
    private Map<String, Map<String,Class<?>>> schema = new HashMap<>();
    private Map<String,List<Map<String,Object>>> data = new HashMap<>();

    public H2DatabaseExtension addTable(String tableName, Map<String,Class<?>> columns) {
        schema.put(tableName, columns);
        return this;
    }

    public H2DatabaseExtension addData(String tableName, List<Map<String,Object>> data) {
        this.data.put(tableName, data);
        return this;
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);

        try (Statement statement = connection.createStatement()) {
            for( String tableName : schema.keySet()) {
                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ").append(tableName)
                        .append(" (id INT PRIMARY KEY AUTO_INCREMENT,");
                for( String columnName : schema.get(tableName).keySet() ) {
                    sb.append(" ").append(columnName).append(" ").append(getType(schema.get(tableName).get(columnName))).append(" NULL,");
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append(")");
                statement.execute(sb.toString());
            }

            for( String tableName : data.keySet() ) {
                StringBuilder sb = new StringBuilder();
                sb.append("INSERT INTO ").append(tableName).append(" (");
                sb.append(String.join(",", schema.get(tableName).keySet()));
                sb.append(") VALUES (");
                sb.append(schema.get(tableName).values().stream().map(v -> "?").collect(Collectors.joining(",")));
                sb.append(")");
                String insertQuery = sb.toString();
                for( Map<String,Object> row : data.get(tableName) ) {
                    getSql().executeInsert(insertQuery,row.values().toArray());
                }
            }
        }
    }

    private String getType(Class<?> clazz) {
        if (clazz.equals(String.class)) {
            return "VARCHAR(255)";
        } else if (clazz.equals(Integer.class)) {
            return "INT";
        } else if (clazz.equals(Long.class)) {
            return "BIGINT";
        } else if (clazz.equals(Boolean.class)) {
            return "BOOLEAN";
        } else if(clazz.equals(Double.class)) {
            return "DOUBLE";
        } else if(clazz.equals(java.sql.Date.class)) {
            return "DATE";
        } else if(clazz.equals(java.sql.Time.class)) {
            return "TIME";
        } else if(clazz.equals(java.sql.Timestamp.class)) {
            return "TIMESTAMP";
        } else {
            return "VARCHAR(255)";
        }
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        try {
            if (connection != null && !connection.isClosed()) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("DROP ALL OBJECTS");
                }
                connection.close();
            }
        } catch (Exception e) {
            System.err.println("Error cleaning up H2 database: " + e.getMessage());
        }
    }

    // Expose connection to tests
    public Connection getConnection() {
        return this.connection;
    }

    public Sql getSql() throws SQLException {
        return Sql.newInstance( JDBC_URL, USER, PASSWORD );
    }
}
