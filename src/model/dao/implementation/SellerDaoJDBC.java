package model.dao.implementation;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

import java.sql.*;
import java.util.List;

public class SellerDaoJDBC implements SellerDao {
    private Connection connection;

    public SellerDaoJDBC(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void insert(Seller seller) {

    }

    @Override
    public void update(Seller seller) {

    }

    @Override
    public void delete(Seller seller) {

    }

    @Override
    public Seller findById(Integer id) {
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            statement = connection.prepareStatement("SELECT seller.*, department.name as department_name "
                    + "FROM seller INNER JOIN department "
                    + "ON seller.department_id = department.id "
                    + "WHERE seller.id = ?",
                    Statement.RETURN_GENERATED_KEYS
            );
            statement.setInt(1, id);
            resultSet = statement.executeQuery();

            if (resultSet.next()) {
                Department department = new Department();
                department.setId(resultSet.getInt("department_id"));
                department.setName(resultSet.getString("department_name"));

                Seller seller = new Seller();
                seller.setId(resultSet.getInt("id"));
                seller.setName(resultSet.getString("name"));
                seller.setEmail(resultSet.getString("email"));
                seller.setBaseSalary(resultSet.getDouble("basesalary"));
                seller.setBirthDate(resultSet.getDate("birthdate"));
                seller.setDepartment(department);

                return seller;
            }
            return null;
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        } finally {
            DB.closeStatement(statement);
            DB.closeResultSet(resultSet);
        }
    }

    @Override
    public List<Seller> findAll() {
        return List.of();
    }
}
