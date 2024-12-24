package model.dao.implementation;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SellerDaoJDBC implements SellerDao {
    private final Connection connection;

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
                Department department = instantiateDepartment(resultSet);

                return instantiateSeller(resultSet, department);
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

    public List<Seller> findByDepartment(Department department) {
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            statement = connection.prepareStatement("SELECT seller.*, department.name as department_name "
                    + "FROM seller INNER JOIN department "
                    + "ON seller.department_id = department.id "
                    + "WHERE department_id = ? "
                    + "ORDER BY name",
                    Statement.RETURN_GENERATED_KEYS
            );
            statement.setInt(1, department.getId());
            resultSet = statement.executeQuery();

            List<Seller> sellers = new ArrayList<>();
            Map<Integer, Department> map = new HashMap<>();
            while (resultSet.next()) {
                Department dep = map.get(resultSet.getInt("department_id"));
                if (dep == null) {
                    dep = instantiateDepartment(resultSet);
                    map.put(resultSet.getInt("department_id"), dep);
                }
                Seller seller = instantiateSeller(resultSet, dep);
                sellers.add(seller);
            }

            return sellers;
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        } finally {
            DB.closeStatement(statement);
            DB.closeResultSet(resultSet);
        }
    }

    private Department instantiateDepartment(ResultSet rs) throws SQLException {
        Department department = new Department();
        department.setId(rs.getInt("department_id"));
        department.setName(rs.getString("department_name"));

        return department;
    }

    private Seller instantiateSeller(ResultSet rs, Department dep) throws SQLException {
        Seller seller = new Seller();
        seller.setId(rs.getInt("id"));
        seller.setName(rs.getString("name"));
        seller.setEmail(rs.getString("email"));
        seller.setBaseSalary(rs.getDouble("basesalary"));
        seller.setBirthDate(rs.getDate("birthdate"));
        seller.setDepartment(dep);

        return seller;
    }
}
