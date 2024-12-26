package application;

import db.DB;
import db.DbException;
import db.DbIntegrityException;
import model.dao.DaoFactory;
import model.dao.DepartmentDao;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        //testSellerDao();
        testDepartmentDao();

        DB.closeConnection();

        /*
        demo1();
        demo2();
        demo3();
        demo4();
        demo5();
         */
    }

    public static void testSellerDao() {
        SellerDao sellerDao = DaoFactory.createSellerDao();

        System.out.println("TEST 1 => FIND BY ID");
        Seller seller = sellerDao.findById(3);
        System.out.println(seller);
        System.out.println();

        System.out.println("TEST 2 => FIND BY DEPARTMENT");
        Department department = new Department(2, null);
        List<Seller> sellers = sellerDao.findByDepartment(department);
        for (Seller s : sellers) {
            System.out.println(s);
            System.out.println();
        }

        System.out.println("TEST 3 => FIND ALL");
        sellers = sellerDao.findAll();
        for (Seller s : sellers) {
            System.out.println(s);
            System.out.println();
        }

        /*
        System.out.println("TEST 4 => SELLER INSERT");
        Seller sellerInsert = new Seller(null, "Greg Cyan", "greg@gmail.com", new Date(), 4000.0, department);
        sellerDao.insert(sellerInsert);
        System.out.println("Inserted successfully, generated id: " + sellerInsert.getId());
        */

        /*
        System.out.println("TEST 5 => SELLER UPDATE");
        seller = sellerDao.findById(1);
        seller.setName("Martha Marge");
        sellerDao.update(seller);
        */

        /*
        System.out.println("TEST 6 => SELLER DELETE BY ID");
        sellerDao.delete(9);
        */
    }

    public static void testDepartmentDao() {
        DepartmentDao departmentDao = DaoFactory.createDepartmentDao();

        System.out.println("TEST 7 => DEPARTMENT FIND BY ID");
        Department department = departmentDao.findById(2);
        System.out.println(department);
        System.out.println();

        /*
        System.out.println("TEST 9 => DEPARTMENT INSERT");
        Department departmentInsert = new Department(null, "Music");
        departmentDao.insert(departmentInsert);
        System.out.println("Inserted successfully, generated id: " + departmentInsert.getId());
        */

        System.out.println("TEST 10 => DEPARTMENT UPDATE");
        department = departmentDao.findById(9);
        department.setName("Games");
        departmentDao.update(department);
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