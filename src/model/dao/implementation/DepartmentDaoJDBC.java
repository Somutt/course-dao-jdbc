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

    public void update(Department department) {}

    public void delete(Integer id) {}

    public Department findById(Integer id) {
        return null;
    }

    public List<Department> findAll() {
        return List.of();
    }
}
