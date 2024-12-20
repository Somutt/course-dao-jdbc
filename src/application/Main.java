package application;

import db.DB;
import db.DbException;
import db.DbIntegrityException;
import model.entities.Department;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Main {
    public static void main(String[] args) {
        Connection connection = DB.getConnection();

        Department department = new Department(1, "Books");
        System.out.println(department);

        DB.closeConnection();

        /*
        demo1();
        demo2();
        demo3();
        demo4();
        demo5();
         */
    }

    public static void demo1() {
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;

        try {
            conn = DB.getConnection();
            st = conn.createStatement();
            rs = st.executeQuery("SELECT * FROM seller");

            while (rs.next()) {
                System.out.println(rs.getInt("id") + ", " + rs.getString("name"));
            }
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        } finally {
            DB.closeResultSet(rs);
            DB.closeStatement(st);
            DB.closeConnection();
        }
    }

    public static void demo2() {
        Connection conn = null;
        PreparedStatement ps = null;
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");

        try {
            conn = DB.getConnection();
            ps = conn.prepareStatement(
                    "INSERT INTO seller "
                    + "(name, email, birthdate, basesalary, department_id) "
                    + "VALUES "
                    + "(?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS
            );
            ps.setString(1, "Damian Yellow");
            ps.setString(2, "damian@gmail.com");
            ps.setDate(3, new java.sql.Date(sdf.parse("15/11/1990").getTime()));
            ps.setDouble(4, 5000.0);
            ps.setInt(5, 2);

            int rowsAffected = ps.executeUpdate();
            if (rowsAffected > 0) {
                ResultSet rs = ps.getGeneratedKeys();
                while (rs.next()) {
                    int id = rs.getInt(1);
                    System.out.println("Done! generated ID is " + id);
                }
            } else {
                System.out.println("No rows affected");
            }
        } catch (SQLException | ParseException e) {
            throw new DbException(e.getMessage());
        } finally {
            DB.closeStatement(ps);
            DB.closeConnection();
        }
    }

    public static void demo3() {
        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = DB.getConnection();
            ps = conn.prepareStatement(
                    "UPDATE seller "
                    + "SET basesalary = basesalary + ? "
                    + "WHERE "
                    + "(department_id = ?)",
                    Statement.RETURN_GENERATED_KEYS
            );
            ps.setDouble(1, 200.0);
            ps.setInt(2, 2);

            int rowsAffected = ps.executeUpdate();
            System.out.println("Done! updated " + rowsAffected + " rows");
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        } finally {
            DB.closeStatement(ps);
            DB.closeConnection();
        }
    }

    public static void demo4() {
        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = DB.getConnection();
            ps = conn.prepareStatement(
                    "DELETE FROM department "
                    + "WHERE "
                    + " id = ?",
                    Statement.RETURN_GENERATED_KEYS
            );
            ps.setInt(1, 2);

            int rowsAffected = ps.executeUpdate();
            System.out.println("Done! deleted " + rowsAffected + " rows");
        } catch (SQLException e) {
            throw new DbIntegrityException(e.getMessage());
        }
    }

    public static void demo5() {
        Connection conn = null;
        Statement st = null;

        try {
            conn = DB.getConnection();
            conn.setAutoCommit(false);
            st = conn.createStatement();

            int rows = st.executeUpdate("UPDATE seller SET basesalary = 2090 WHERE department_id = 1");

            int x = 1;
            if (x < 2) {
                throw new SQLException("Forced error");
            }

            int newRows = st.executeUpdate("UPDATE seller SET basesalary = 3090 WHERE department_id = 2");

            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
                throw new DbException("Transaction rolled back, caused by: " + e.getMessage());
            } catch (SQLException e1) {
                throw new DbException("Error trying to rollback transaction, caused by: " + e1.getMessage());
            }
        } finally {
            DB.closeStatement(st);
            DB.closeConnection();
        }
    }

}