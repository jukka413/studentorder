package edu.javacourse.studentorder.dao;

import edu.javacourse.studentorder.config.Config;
import edu.javacourse.studentorder.domain.*;
import edu.javacourse.studentorder.exception.DaoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.MarshalledObject;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StudentOrderDaoImpl implements StudentOrderDao {

    public static final Logger logger = LoggerFactory.getLogger(StudentOrderDaoImpl.class);

    private static final String INSERT_ORDER =
            "INSERT INTO public.jc_student_order" +
                    "(student_order_status, student_order_date, h_sur_name, h_given_name, h_patronymic, h_date_of_birth" +
                    ", h_passport_seria, h_passport_number, h_passport_date, h_passport_office_id, h_post_index" +
                    ", h_street_code, h_building, h_extension, h_apartment, h_university_id, h_student_number, w_sur_name" +
                    ", w_given_name, w_patronymic, w_date_of_birth, w_passport_seria, w_passport_number, w_passport_date" +
                    ", w_passport_office_id, w_post_index, w_street_code, w_building, w_extension, w_apartment" +
                    ", w_university_id, w_student_number, certificate_id, register_office_id, marriage_date)" +
                    "VALUES (?, ?, ?, " +
                    " ?, ?, ?, ?, " +
                    " ?, ?, ?, ?, " +
                    " ?, ?, ?, ?, ?, " +
                    " ?, ?, ?, ?, " +
                    " ?, ?, ?, ?, " +
                    " ?, ?, ?, ?, ?, " +
                    " ?, ?, ?, ?, ?, ?);";

    private static final String INSERT_CHILD =
            "INSERT INTO public.jc_student_child" +
                    "(student_order_id, c_sur_name, c_given_name, c_patronymic, c_date_of_birth, c_certificate_number" +
                    ", c_certificate_date, c_register_office_id, c_post_index, c_street_code, c_building, c_extension, c_apartment)" +
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
    private static final String SELECT_ORDERS =
            "select jso.*,jro.r_office_area_id, jro.r_office_name" +
                    ", jpo1.p_office_area_id as h_p_office_area_id,jpo1.p_office_name as h_p_office_name" +
                    ", jpo2.p_office_area_id as w_p_office_area_id,jpo2.p_office_name as w_p_office_name\n" +
                    "from jc_student_order jso, jc_register_office jro ,jc_passport_office jpo1 ,jc_passport_office jpo2\n" +
                    "where jso.register_office_id =jro.r_office_id \n" +
                    "and jso.h_passport_office_id =jpo1.p_office_id \n" +
                    "and jso.w_passport_office_id =jpo2.p_office_id \n" +
                    "and student_order_status = ? order by jso.student_order_date limit ?";
    public static final String SELECT_CHILD =
            "select jsc.*,jro.r_office_area_id, jro.r_office_name from jc_student_child jsc ,jc_register_office jro" +
                    " where jsc.c_register_office_id =jro.r_office_id and jsc.student_order_id in ";
    private static final String SELECT_ORDERS_FULL=
            "select jso.*,jro.r_office_area_id, jro.r_office_name\n" +
                    ", jpo1.p_office_area_id as h_p_office_area_id,jpo1.p_office_name as h_p_office_name\n" +
                    ", jpo2.p_office_area_id as w_p_office_area_id,jpo2.p_office_name as w_p_office_name\n" +
                    ", jsc.*,jro_ch.r_office_area_id , jro_ch.r_office_name \n" +
                    "from jc_student_order jso, jc_register_office jro ,jc_passport_office jpo1 \n" +
                    ",jc_passport_office jpo2, jc_student_child jsc, jc_register_office jro_ch \n" +
                    "where jso.register_office_id =jro.r_office_id \n" +
                    "and jso.h_passport_office_id =jpo1.p_office_id \n" +
                    "and jso.w_passport_office_id =jpo2.p_office_id\n" +
                    "and jso.student_order_id =jsc.student_order_id \n" +
                    "and jsc.c_register_office_id =jro_ch.r_office_id \n" +
                    "and student_order_status = ? order by jso.student_order_id limit ?";

    private Connection getConnection() throws SQLException {
        return ConnectionBuilder.getConnection();
    }
    @Override
    public Long saveStudentOrder(StudentOrder so) throws DaoException {
        Long result = -1L;

        logger.info("SO:{}", so);

        try (Connection con = getConnection();
             PreparedStatement stmt = con.prepareStatement(INSERT_ORDER, new String[]{"student_order_id"})) {
            con.setAutoCommit(false);

            try {
                // Header
                stmt.setInt(1, StudentOrderStatus.START.ordinal());
                stmt.setTimestamp(2, java.sql.Timestamp.valueOf(LocalDateTime.now()));

                // Husband and Wife
                setParamsForAdult(stmt, 3, so.getHusband());
                setParamsForAdult(stmt, 18, so.getWife());

                // Marriage
                stmt.setString(33, so.getMarriageCertificateId());
                stmt.setLong(34, so.getMarriageOffice().getOfficeId());
                stmt.setDate(35, java.sql.Date.valueOf(so.getMarriageDate()));

                stmt.executeUpdate();

                ResultSet gkRs = stmt.getGeneratedKeys();
                if (gkRs.next()) {
                    result = gkRs.getLong(1);
                }
                gkRs.close();

                saveChildren(con, so, result);
                con.commit();
            } catch (SQLException ex){
                con.rollback();
                throw ex;

            }

        } catch (SQLException ex) {
            logger.error(ex.getMessage(), ex);
            throw new DaoException(ex);
        }

        return result;
    }

    private void saveChildren(Connection con, StudentOrder so, Long soId) throws SQLException {
        try (PreparedStatement stmt = con.prepareStatement(INSERT_CHILD)) {

            for (Child child : so.getChildren()) {
                stmt.setLong(1,soId);
                setParamsForChild(stmt,child);
                stmt.executeUpdate();

            }
        }
    }

    private void setParamsForAdult(PreparedStatement stmt, int start, Adult adult) throws SQLException {

        setParamsForPerson(stmt, start, adult);
        stmt.setString(start+4, adult.getPassportSeria());
        stmt.setString(start+5, adult.getPassportNumber());
        stmt.setDate(start+6, Date.valueOf(adult.getIssueDate()));
        stmt.setLong(start+7, adult.getIssueDepartment().getOfficeId());
        stmt.setLong(start+13,adult.getUniversity().getUniversityId());
        stmt.setString(start+14,adult.getStudentId());

        setParamsForAddress(stmt, start, adult);
    }

    private void setParamsForChild(PreparedStatement stmt, Child child) throws SQLException {
        setParamsForPerson(stmt,2,child);
        stmt.setString(6,child.getCertificateNumber());
        stmt.setDate(7,java.sql.Date.valueOf(child.getIssueDate()));
        stmt.setLong(8,child.getIssueDepartment().getOfficeId());
        setParamsForAddress(stmt,1,child);
    }

    private void setParamsForPerson(PreparedStatement stmt, int start, Person person) throws SQLException {
        stmt.setString(start, person.getSurName());
        stmt.setString(start+1, person.getGivenName());
        stmt.setString(start+2, person.getPatronymic());
        stmt.setDate(start+3, Date.valueOf(person.getDateOfBirth()));
    }
    
    private void setParamsForAddress(PreparedStatement stmt, int start, Person person) throws SQLException {
        Address address = person.getAddress();
        stmt.setString(start+8, address.getPostCode());
        stmt.setLong(start+9, address.getStreet().getStreetCode());
        stmt.setString(start+10, address.getBuilding());
        stmt.setString(start+11, address.getExtension());
        stmt.setString(start+12, address.getApartment());
    }

    @Override
    public List<StudentOrder> getStudentOrders() throws DaoException {
        return getStudentOrdersOneSelect();
    }

    private List<StudentOrder> getStudentOrdersOneSelect() throws DaoException {
        List<StudentOrder> result = new LinkedList<>();
        try (Connection con = getConnection();
             PreparedStatement stmt = con.prepareStatement(SELECT_ORDERS_FULL)){
            Map<Long,StudentOrder> maps = new HashMap<>();

            stmt.setInt(1, StudentOrderStatus.START.ordinal());
            int limit = Integer.parseInt(Config.getProperty(Config.DB_LIMIT));
            stmt.setInt(2,limit);

            ResultSet rs = stmt.executeQuery();
            int counter = 0;
            while (rs.next()){
                Long soId = rs.getLong("student_order_id");
                if(!maps.containsKey(soId)) {
                    StudentOrder so = getFullStudentOrder(rs);

                    result.add(so);
                    maps.put(soId,so);
                }
                StudentOrder so = maps.get(soId);
                so.addChild(fillChild(rs));
                counter++;
            }
            //так как при превышении лимита есть веротяность того, что последняя семья не полная - удаляем ее из списка
            if (counter>=limit) {
                result.remove(result.size()-1);
            }
            rs.close();

        } catch (SQLException ex) {
            throw new DaoException(ex);
        }

        return result;
    }

    private List<StudentOrder> getStudentOrdersTwoSelect() throws DaoException {
        List<StudentOrder> result = new LinkedList<>();
        try (Connection con = getConnection();
             PreparedStatement stmt = con.prepareStatement(SELECT_ORDERS)){
            stmt.setInt(1, StudentOrderStatus.START.ordinal());
            stmt.setInt(2,Integer.parseInt(Config.getProperty(Config.DB_LIMIT)));
            ResultSet rs = stmt.executeQuery();
            List<Long> ids = new LinkedList<>();

            while (rs.next()){
                StudentOrder so = getFullStudentOrder(rs);

                result.add(so);
                ids.add(so.getStudentOrderId());
            }
            findChildren(con,result);

            rs.close();

        } catch (SQLException ex) {
            throw new DaoException(ex);
        }

        return result;
    }

    private StudentOrder getFullStudentOrder(ResultSet rs) throws SQLException {
        StudentOrder so = new StudentOrder();
        fillStudentOrder(rs, so);
        fillMarriage(rs, so);
        so.setHusband(fillAdult(rs, "h_"));
        so.setWife(fillAdult(rs, "w_"));
        return so;
    }

    //TODO Разобраться с тем, что и как делает этот метод
    private void findChildren(Connection con, List<StudentOrder> result) throws SQLException {
        String cl = "(" + result.stream().map(so -> String.valueOf(so.getStudentOrderId()))
                .collect(Collectors.joining(",")) + ")";

        Map<Long, StudentOrder> maps = result.stream().collect(Collectors
                .toMap(so -> so.getStudentOrderId(), so -> so));

        try (PreparedStatement stmt = con.prepareStatement(SELECT_CHILD + cl)){
            ResultSet rs = stmt.executeQuery();
            while (rs.next()){
                Child ch = fillChild(rs);
                StudentOrder so = maps.get(rs.getLong("student_order_id"));
                so.addChild(ch);
            }

        }
    }

    private Child fillChild(ResultSet rs) throws SQLException {
        String surName = rs.getString("c_sur_name");
        String givenName = rs.getString("c_given_name");
        String patronymic = rs.getString("c_patronymic");
        LocalDate dateOfBirth = rs.getDate("c_date_of_birth").toLocalDate();
        Child child = new Child(surName,givenName,patronymic,dateOfBirth);
        child.setCertificateNumber(rs.getString("c_certificate_number"));
        child.setIssueDate(rs.getDate("c_certificate_date").toLocalDate());

        Long roId = rs.getLong("c_register_office_id");
        String roArea = rs.getString("r_office_area_id");
        String roName = rs.getString("r_office_name");
        RegisterOffice ro = new RegisterOffice(roId,roArea,roName);
        child.setIssueDepartment(ro);

        Address adr = new Address();
        Street street = new Street(rs.getLong("c_street_code"),"");
        adr.setStreet(street);
        adr.setPostCode(rs.getString("c_post_index"));
        adr.setBuilding(rs.getString( "c_building"));
        adr.setExtension(rs.getString("c_extension"));
        adr.setApartment(rs.getString("c_apartment"));
        child.setAddress(adr);
        return child;

    }

    private void fillStudentOrder(ResultSet rs, StudentOrder so) throws SQLException {
        so.setStudentOrderId(rs.getLong("student_order_id"));
        so.setStudentOrderDate(rs.getTimestamp("student_order_date").toLocalDateTime());
        so.setStudentOrderStatus(StudentOrderStatus.fromValue(rs.getInt("student_order_status")));

    }

    private void fillMarriage(ResultSet rs, StudentOrder so) throws SQLException {
        so.setMarriageCertificateId(rs.getString("certificate_id"));
        so.setMarriageDate(rs.getDate("marriage_date").toLocalDate());

        Long roId = rs.getLong("register_office_id");
        String areaId = rs.getString("r_office_area_id");
        String name = rs.getString("r_office_name");
        RegisterOffice ro = new RegisterOffice(roId,areaId,name);
        so.setMarriageOffice(ro);

    }

    private Adult fillAdult(ResultSet rs, String pref) throws SQLException {
        Adult adult = new Adult();
        adult.setSurName(rs.getString(pref + "sur_name"));
        adult.setGivenName(rs.getString(pref + "given_name"));
        adult.setPatronymic(rs.getString(pref + "patronymic"));
        adult.setDateOfBirth(rs.getDate(pref + "date_of_birth").toLocalDate());
        adult.setPassportSeria(rs.getString(pref + "passport_seria"));
        adult.setPassportNumber(rs.getString(pref + "passport_number"));
        adult.setIssueDate(rs.getDate(pref+"passport_date").toLocalDate());

        Long poId = rs.getLong(pref+"passport_office_id");
        String poArea = rs.getString(pref+ "p_office_area_id");
        String poName = rs.getString(pref+"p_office_name");
        PassportOffice po = new PassportOffice(poId,poArea,poName);
        adult.setIssueDepartment(po);
        Address adr = new Address();
        Street street = new Street(rs.getLong(pref+"street_code"),"");
        adr.setStreet(street);
        adr.setPostCode(rs.getString(pref+"post_index"));
        adr.setBuilding(rs.getString(pref + "building"));
        adr.setExtension(rs.getString(pref + "extension"));
        adr.setApartment(rs.getString(pref + "apartment"));
        adult.setAddress(adr);

        University univer = new University(rs.getLong(pref+ "university_id"),"");
        adult.setUniversity(univer);
        adult.setStudentId(rs.getString(pref + "student_number"));

        return adult;
    }
}

