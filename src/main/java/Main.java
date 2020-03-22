import java.io.*;
import java.sql.*;
import java.util.*;

public class Main {
    private static final String DEFAULT_DRIVER = "org.postgresql.Driver";
    private static final String DEFAULT_URL = "jdbc:postgresql://localhost:5432/part4";
    private static final String DEFAULT_USERNAME = "part4";
    private static final String DEFAULT_PASSWORD = "part4_GfhjkzYtn321";
    private static String BASE_DIR = "./log/";
    private static Connection connection = null;
    private static String brandName;
    private static String modelName;
    private static String errorText;

    public static void main(String[] args) throws IOException {
        String FILE_PATH = "./parse/parts/";
        String driver = ((args.length > 0) ? args[0] : DEFAULT_DRIVER);
        String url = ((args.length > 1) ? args[1] : DEFAULT_URL);
        String username = ((args.length > 2) ? args[2] : DEFAULT_USERNAME);
        String password = ((args.length > 3) ? args[3] : DEFAULT_PASSWORD);

        try {
            connection = createConnection(driver, url, username, password);
            System.out.println("Server connect");
            connection.setAutoCommit(false);
            SaveFile.SaveData(BASE_DIR, "log", "Server connect ", true);

        } catch (ClassNotFoundException | SQLException e) {
            System.out.println("Error connect");
            SaveFile.SaveData(BASE_DIR, "log", "Error connect", true);
            e.printStackTrace();
        }


        File pathList = new File(FILE_PATH);
        File[] files = pathList.listFiles();

        assert files != null;
        Arrays.sort(files);


        for (File dir : files) {
            brandName = dir.getName();
            System.out.println(brandName);

            File[] arrFiles = dir.listFiles();
            assert arrFiles != null;
            Arrays.sort(arrFiles);

            for (File file : arrFiles) {
                modelName = file.getName();

                if (modelName.contains(".csv")) {
                    modelName = modelName.replaceAll(".csv+$", "");
                    System.out.println(modelName);
                    try {
                        String sqlSelect = "SELECT * FROM insert_detail(?, ?, ?, ?, ?, ?, ?)";
                        List sqlParam = Arrays.asList(brandName, modelName, null, null, modelName, null, null);
                        query(connection, sqlSelect, sqlParam);
                        connection.commit();

                    } catch (SQLException e) {
                        e.printStackTrace();
                        errorText = "SELECT * FROM insert_detail(" + brandName + ", " + modelName + ", null, null, " + modelName + ", null, null)";
                        SaveFile.SaveData(BASE_DIR, "log", "error code: " + e.getErrorCode() + errorText, true);
                        System.out.println("SQL error");
                        try {
                            connection.rollback();
                        } catch (SQLException e1) {
                            e1.printStackTrace();
                            errorText = "SELECT * FROM insert_detail(" + brandName + ", " + modelName + ", null, null, " + modelName + ", null, null)";
                            SaveFile.SaveData(BASE_DIR, "log", "error code: " + e1.getErrorCode() + errorText, true);
                            System.out.println("SQL error");
                        }
                    }
                    inner(file);
                    SaveFile.SaveData(BASE_DIR, "parse_models", modelName, true);
                }
            }
        }
    }

    private static void inner(File file) throws IOException {
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(file.getAbsolutePath());
        } catch (FileNotFoundException e) {
            System.out.println("Not found " + file.getName());
            SaveFile.SaveData(BASE_DIR, "log", "file note found: " + file.getName(), true);
            e.printStackTrace();
        }

        assert fileInputStream != null;
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
        String strLine;
        String[] subStr;

        while ((strLine = bufferedReader.readLine()) != null) {
            String moduleName = "";
            String partCode = "";
            String partName = "";
            String description = "";
            String module_image = "";

            subStr = strLine.split(";");
            for (int i = 0; i < subStr.length; i++) {
                switch (i) {
                    case 0:
                        moduleName = subStr[0].trim();
                        break;
                    case 1:
                        partCode = subStr[1].trim();
                        break;
                    case 2:
                        partName = subStr[2].trim();
                        break;
                    case 3:
                        description = subStr[3].trim();
                        break;
                    case 4:
                        module_image = subStr[4].trim();
                        break;
                }
            }

            try {
                if (moduleName.equals("")) {
                    moduleName = null;
                }
                if (partCode.equals("")) {
                    partCode = null;
                }
                if (partName.equals("")) {
                    partName = null;
                }
                if (description.equals("")) {
                    description = null;
                }
                if (module_image.equals("")) {
                    module_image = null;
                }

                String sqlSelect = "SELECT * FROM insert_detail(?, ?, ?, ?, ?, ?, ?)";

                List sqlParam = Arrays.asList(brandName, modelName, moduleName, partCode, partName, ' ', module_image);
//                System.out.println(brandName + ", " + modelName + ", " + moduleName + ", " + partCode + ", " + partName + ", " + description + ", " + module_image);
                query(connection, sqlSelect, sqlParam);
                connection.commit();

                if (description != null) {
                    sqlSelect = "SELECT description FROM partcodes WHERE code = ?";
                    sqlParam = Arrays.asList(partCode);
                    String res = getDescription(connection, sqlSelect, sqlParam);
                    connection.commit();

                    if (res != null) {
                        if (!res.equals(description + ";")) {
                            res += description + ";";

                            sqlSelect = "UPDATE partcodes SET description = ? WHERE code = ?";
                            sqlParam = Arrays.asList(res, partCode);
                            update(connection, sqlSelect, sqlParam);
                            connection.commit();
                        }
                    } else {
                        sqlSelect = "UPDATE partcodes SET description = ? WHERE code = ?";
                        sqlParam = Arrays.asList(description + ";", partCode);
                        update(connection, sqlSelect, sqlParam);
                        connection.commit();
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
                errorText = "SELECT * FROM insert_detail(" + brandName + ", " + modelName + ", " + moduleName + ", " + partCode + ", " + partName + ", " + description + ")";
                SaveFile.SaveData(BASE_DIR, "log", "error code: " + e.getErrorCode() + errorText, true);
                System.out.println("SQL error");
                try {
                    connection.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                    errorText = "SELECT * FROM insert_detail(" + brandName + ", " + modelName + ", " + moduleName + ", " + partCode + ", " + partName + ", " + description + ")";
                    SaveFile.SaveData(BASE_DIR, "log", "error code: " + e1.getErrorCode() + errorText, true);
                    System.out.println("SQL error");
                }
            }
        }
    }

    private static int query(Connection connection, String sql, List<Object> parameters) throws SQLException {
        int results = 0;
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql);
            int i = 0;
            for (Object parameter : parameters) {
                ps.setObject(++i, parameter);
            }
            ps.executeQuery();
        } finally {
            close(ps);
        }
        return results;
    }

    private static String getDescription(Connection connection, String sql, List<Object> parameters) throws SQLException {
        String results = "";
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(sql);
            int i = 0;
            for (Object parameter : parameters) {
                ps.setObject(++i, parameter);
            }
            rs = ps.executeQuery();
            while (rs.next()) {
                results = rs.getString("description");
            }
        } finally {
            close(rs);
            close(ps);
        }
        return results;
    }

    private static void update(Connection connection, String sql, List<Object> parameters) throws SQLException {
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql);
            int i = 0;
            for (Object parameter : parameters) {
                ps.setObject(++i, parameter);
            }
            ps.execute();
        } finally {
            close(ps);
        }
    }


    private static Connection createConnection(String driver, String url, String username, String password) throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        if ((username == null) || (password == null) || (username.trim().length() == 0) || (password.trim().length() == 0)) {
            return DriverManager.getConnection(url);
        } else {
            return DriverManager.getConnection(url, username, password);
        }
    }

    private static void close(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void close(Statement st) {
        try {
            if (st != null) {
                st.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void close(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static List<Map<String, Object>> map(ResultSet rs) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        try {
            if (rs != null) {
                ResultSetMetaData meta = rs.getMetaData();
                int numColumns = meta.getColumnCount();

                while (rs.next()) {
                    Map<String, Object> row = new HashMap<String, Object>();

                    for (int i = 1; i <= numColumns; ++i) {
                        String name = meta.getColumnName(i);
                        Object value = rs.getObject(i);
                        row.put(name, value);
                    }
                    results.add(row);
                }
            }
        } finally {
            close(rs);
        }
        return results;
    }
}
