package model.dao.implementation;

import db.DB;
import db.DbException;
import model.dao.DepartmentDao;
import model.entities.Department;

import java.sql.*;
import java.util.List;

public class DepartmentDaoJDBC implements DepartmentDao {
    private final Connection connection;

    public DepartmentDaoJDBC(Connection connection) {
        this.connection = connection;
    }

    public void insert(Department department) {
        PreparedStatement statement = null;

        try {
            statement = connection.prepareStatement("INSERT INTO department "
                    + "(name) "
                    + "VALUES "
                    + "(?)",
                    Statement.RETURN_GENERATED_KEYS
            );
            statement.setString(1, department.getName());
            int rowsAffected = statement.executeUpdate();

            if (rowsAffected > 0) {
                ResultSet resultSet = statement.getGeneratedKeys();
                if (resultSet.next()) {
                    department.setId(resultSet.getInt(1));
                }
                DB.closeResultSet(resultSet);
            } else {
                throw new DbException("Unexpected error, no rows affected");
            }
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        } finally {
            DB.closeStatement(statement);
        }
    }

    public void update(Department department) {
        PreparedStatement statement = null;

        try {
            statement = connection.prepareStatement("UPDATE department "
                    + "SET name = ? "
                    + "WHERE id = ?",
                    Statement.RETURN_GENERATED_KEYS
            );
            statement.setString(1, department.getName());
            statement.setInt(2, department.getId());

            int rowsAffected = statement.executeUpdate();
            System.out.println("Updated successfully, " + rowsAffected + " rows affected");
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        } finally {
            DB.closeStatement(statement);
        }
    }

    public void delete(Integer id) {}

    public Department findById(Integer id) {
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            statement = connection.prepareStatement("SELECT department.* "
                + "FROM department "
                + "WHERE department.id = ?",
                Statement.RETURN_GENERATED_KEYS
            );
            statement.setInt(1, id);
            resultSet = statement.executeQuery();

            if (resultSet.next()) {
                return instantiateDepartment(resultSet);
            }
            return null;
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        } finally {
            DB.closeStatement(statement);
            DB.closeResultSet(resultSet);
        }
    }

    public List<Department> findAll() {
        return List.of();
    }

    private Department instantiateDepartment(ResultSet rs) throws SQLException {
        Department department = new Department();
        department.setId(rs.getInt("id"));
        department.setName(rs.getString("name"));

        return department;
    }
}
