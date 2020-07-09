package edu.javacourse.studentorder.dao;

import edu.javacourse.studentorder.config.Config;
import edu.javacourse.studentorder.domain.CountryArea;
import edu.javacourse.studentorder.domain.PassportOffice;
import edu.javacourse.studentorder.domain.RegisterOffice;
import edu.javacourse.studentorder.domain.Street;
import edu.javacourse.studentorder.exception.DaoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class DictionaryDaoImpl implements DictionaryDao
{
    public static final Logger logger = LoggerFactory.getLogger(DictionaryDaoImpl.class);
    private static final String GET_STREET = "select street_code, street_name " +
            "from jc_street where UPPER(street_name) like UPPER(?)";

    private static final String GET_PASSPORT = "select * " +
            "from jc_passport_office where p_office_area_id = ?";

    private static final String GET_REGISTER = "select * " +
            "from jc_register_office where r_office_area_id = ?";

    public static final String GET_AREA = "select * " +
            "from jc_country_struct where area_id like ? and area_id<>?";

    private Connection getConnection() throws SQLException {
        return ConnectionBuilder.getConnection();
    }

    public List<Street> findStreets(String pattern) throws DaoException{
        List<Street> result = new LinkedList<>();

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(GET_STREET)){

            stmt.setString(1,"%"+pattern+"%");
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                Street street = new Street(rs.getLong("street_code"),
                                           rs.getString("street_name"));
                result.add(street);
            }
        } catch (SQLException e){
            logger.error(e.getMessage(), e);
            throw new DaoException(e);
        }
        return result;
    }

    @Override
    public List<PassportOffice> findPassportOffice(String areaId) throws DaoException {
        List<PassportOffice> result = new LinkedList<>();

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(GET_PASSPORT)){

            stmt.setString(1,areaId);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                PassportOffice po = new PassportOffice(rs.getLong("p_office_id"),
                                                       rs.getString("p_office_area_id"),
                                                       rs.getString("p_office_name"));
                result.add(po);
            }
        } catch (SQLException e){
            logger.error(e.getMessage(), e);
            throw new DaoException(e);
        }
        return result;
    }

    @Override
    public List<RegisterOffice> findRegisterOffice(String areaId) throws DaoException {
        List<RegisterOffice> result = new LinkedList<>();

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(GET_REGISTER)){

            stmt.setString(1,areaId);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                RegisterOffice ro = new RegisterOffice(rs.getLong("r_office_id"),
                        rs.getString("r_office_area_id"),
                        rs.getString("r_office_name"));
                result.add(ro);
            }
        } catch (SQLException e){
            logger.error(e.getMessage(), e);
            throw new DaoException(e);
        }
        return result;
    }

    @Override
    public List<CountryArea> findAreas(String areaId) throws DaoException {
        List<CountryArea> result = new LinkedList<>();

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(GET_AREA)){
            String param1 = buildParam(areaId);
            stmt.setString(1,param1);
            stmt.setString(2,areaId);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                CountryArea ca = new CountryArea(rs.getString("area_id"),
                                                 rs.getString("area_name"));
                result.add(ca);
            }
        } catch (SQLException e){
            logger.error(e.getMessage(), e);
            throw new DaoException(e);
        }
        return result;
    }

    private String buildParam(String areaId) throws SQLException {
        if (areaId == null || areaId.trim().isEmpty()) {
            return "__0000000000";
        } else if (areaId.endsWith("0000000000")) {
            return areaId.substring(0, 2) + "___0000000";
        } else if (areaId.endsWith("0000000")) {
            return areaId.substring(0, 5) + "___0000";
        } else if (areaId.endsWith("0000")) {
            return areaId.substring(0, 8) + "____";
        }
        throw new SQLException("Invalid parameter 'areaId':" + areaId);
    }
}
