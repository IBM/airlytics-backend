package com.ibm.airlytics.utilities.validationService.queryservice.queryHandler;


import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySqlQueryHandler extends QueryHandler {

    private Connection conn = null;

    private static Map<Integer,String> eventNamesMap = new HashMap();

    static {
        eventNamesMap.put(0, "app-crash");
        eventNamesMap.put(1, "app-start");
        eventNamesMap.put(2, "page-viewed");
        eventNamesMap.put(3, "purchase-attempted");
        eventNamesMap.put(4, "session-start");
        eventNamesMap.put(5, "session-end");
        eventNamesMap.put(6, "card-clicked");
    }

    public MySqlQueryHandler() throws SQLException {
        if (this.conn == null){
            try {
                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/analytics", "root", "1234");
            } catch (ClassNotFoundException e) {
                System.out.println(e.getMessage());;
            }
        }
    }

    public long submitQueryAndGetCountResult(String query, String table, String day, String condition) throws InterruptedException {
        if (!condition.isEmpty()){
            condition = " and " + condition;
        }
        try {
            return processCountQuery(query, table, day, condition);
        } catch (SQLException throwables) {
            System.out.println(throwables.getMessage());
            return -1;
        }
    }

    public List<List<String>> submitEventsCounterQueryAndGeResults(String table, String day) throws InterruptedException {
        return submitQueryAndGeResults(String.format(Constants.SAMPLE_EVENTS_COUNT_QUERY, table, day), false);
    }

    public List<List<String>> submitQueryAndGeResults(String query) throws InterruptedException {
        return submitQueryAndGeResults(query, true);
    }

    public List<List<String>> submitQueryAndGeResults(String query, boolean includeHeaders) throws InterruptedException {
        return processRowResults(query, includeHeaders);
    }

    private long processCountQuery(String query, String table, String day, String condition) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        int rowCount = 0;
        if (rs.last()) {//make cursor to point to the last row in the ResultSet object
            rowCount = rs.getRow();
            rs.beforeFirst(); //make cursor to point to the front of the ResultSet object, just before the first row.
        }
        return rowCount;
    }

    /**
     * This code calls Athena and retrieves the results of a query.
     * The query must be in a completed state before the results can be retrieved and
     * paginated. The first row of results are the column headers.
     */
    public List<List<String>> processRowResults(String queryString, boolean includeHeaders) {
        List<List<String>> valuesList = new ArrayList<>();
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (SQLException throwables) {
            System.out.println(throwables.getMessage());;
            return null;
        }
        try {
            ResultSet rs = stmt.executeQuery(queryString);
            while (rs.next()) {
                List<String> values = new ArrayList<>();
                ResultSetMetaData rsmd = rs.getMetaData();
                int numOfCols = rsmd.getColumnCount();
                for (int i=1;i<=numOfCols;i++){
                    values.add(rs.getString(i));
                }
                valuesList.add(values);
            }
        } catch (SQLException throwables) {
            System.out.println(throwables.getMessage());;
            return valuesList;
        }
        return valuesList;
    }

    @Override
    public void loadSampleData(String dbName, String tableName) {
        super.loadSampleData(dbName, tableName);
        String query = "Truncate table " + dbName + "." + tableName;
        //Executing the query
        Statement stmt;
        try {
            stmt = conn.createStatement();
            stmt.execute(query);
            for (int i=0;i<100;i++){
                String insertStatement = "insert into " + dbName + "." + tableName + " values ('" + getDayAgoString(1,false) + "', '" + eventNamesMap.get(i%6)+ "','1.0','testdata', 'id" + i + "');";
                stmt.execute(insertStatement);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }
}
