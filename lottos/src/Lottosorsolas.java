import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Lottosorsolas {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java Lottosorsolas <gamers.csv> <data5.csv>");
            return;
        }

        String gamersFile = args[0];
        String dataFile = args[1];

        Connection connection = null;
        try {
            Class.forName("org.mariadb.jdbc.Driver");

            connection = DriverManager.getConnection("jdbc:mariadb://localhost:3306/lottosorsolas", "root", "");

            
            List<String> gamers = readCSV(gamersFile);
            int gamersImported = importGamers(connection, gamers);
            System.out.println("Successfully imported " + gamersImported + " gamers.");

            
            List<String[]> data = readCSVWithHeaders(dataFile);
            int dataImported = importData(connection, data);
            System.out.println("Successfully imported " + dataImported + " data rows.");

            
            displayGamers(connection);

            
            displayData(connection);

        } catch (ClassNotFoundException e) {
            System.out.println("MariaDB JDBC Driver not found. Include it in your library path.");
            e.printStackTrace();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static List<String> readCSV(String filePath) throws IOException {
        List<String> lines = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String line;
        while ((line = reader.readLine()) != null) {
            lines.add(line);
        }
        reader.close();
        return lines;
    }

    private static List<String[]> readCSVWithHeaders(String filePath) throws IOException {
        List<String[]> lines = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String line;
        boolean isHeader = true;
        while ((line = reader.readLine()) != null) {
            if (isHeader) {
                isHeader = false;
                continue;
            }
            lines.add(line.split(";"));
        }
        reader.close();
        return lines;
    }

    private static int importGamers(Connection connection, List<String> gamers) throws SQLException {
        String insertSQL = "INSERT INTO Gamers (name) VALUES (?)";
        PreparedStatement statement = connection.prepareStatement(insertSQL);
        int count = 0;
        for (String gamer : gamers) {
            if (gamer.equals("Name")) continue; // Skip header
            statement.setString(1, gamer);
            count += statement.executeUpdate();
        }
        return count;
    }

    private static int importData(Connection connection, List<String[]> data) throws SQLException {
        String insertSQL = "INSERT INTO Data (gamersId, sz1, sz2, sz3, sz4, sz5) VALUES (?, ?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(insertSQL);
        int count = 0;
        for (String[] row : data) {
            statement.setInt(1, Integer.parseInt(row[0]));
            statement.setInt(2, Integer.parseInt(row[1]));
            statement.setInt(3, Integer.parseInt(row[2]));
            statement.setInt(4, Integer.parseInt(row[3]));
            statement.setInt(5, Integer.parseInt(row[4]));
            statement.setInt(6, Integer.parseInt(row[5]));
            count += statement.executeUpdate();
        }
        return count;
    }

    private static void displayGamers(Connection connection) throws SQLException {
        String query = "SELECT * FROM Gamers";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);

        System.out.println("Gamers:");
        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            System.out.println(id + ": " + name);
        }
    }

    private static void displayData(Connection connection) throws SQLException {
        String query = "SELECT * FROM Data";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);

        System.out.println("Data:");
        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            int gamersId = resultSet.getInt("gamersId");
            int sz1 = resultSet.getInt("sz1");
            int sz2 = resultSet.getInt("sz2");
            int sz3 = resultSet.getInt("sz3");
            int sz4 = resultSet.getInt("sz4");
            int sz5 = resultSet.getInt("sz5");
            System.out.println(id + ": " + gamersId + ", " + sz1 + ", " + sz2 + ", " + sz3 + ", " + sz4 + ", " + sz5);
        }
    }
}
