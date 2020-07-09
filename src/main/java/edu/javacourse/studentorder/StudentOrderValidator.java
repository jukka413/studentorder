package edu.javacourse.studentorder;

import edu.javacourse.studentorder.dao.StudentOrderDaoImpl;
import edu.javacourse.studentorder.domain.*;
import edu.javacourse.studentorder.domain.children.AnswerChildren;
import edu.javacourse.studentorder.domain.register.AnswerCityRegister;
import edu.javacourse.studentorder.domain.student.AnswerStudent;
import edu.javacourse.studentorder.domain.wedding.AnswerWedding;
import edu.javacourse.studentorder.exception.DaoException;
import edu.javacourse.studentorder.mail.MailSender;
import edu.javacourse.studentorder.validator.ChildrenValidator;
import edu.javacourse.studentorder.validator.CityRegisterValidator;
import edu.javacourse.studentorder.validator.StudentValidator;
import edu.javacourse.studentorder.validator.WeddingValidator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

public class StudentOrderValidator {

    private CityRegisterValidator cityRegisterVal;
    private WeddingValidator weddingVal;
    private ChildrenValidator childrenVal;
    private StudentValidator studentVal;
    private MailSender mailSender;

    public StudentOrderValidator() {
        cityRegisterVal = new CityRegisterValidator();
        weddingVal = new WeddingValidator();
        childrenVal = new ChildrenValidator();
        studentVal = new StudentValidator();
        mailSender = new MailSender();
    }

    public static void main(String[] args) throws Exception {

        StudentOrderValidator sov = new StudentOrderValidator();
        sov.checkAll();
    }

    public void checkAll() {
        try {
            List<StudentOrder> soList = readStudentOrders();
            for (StudentOrder so : soList){        //Цикл for each Цикл сам отслеживает пустоту массива и
                checkOneOrder(so);                  //бежит от первого до последного эл-та
            }
        }catch (Exception e) {
                e.printStackTrace();
        }
    }


    public List<StudentOrder> readStudentOrders() throws DaoException {
          return new StudentOrderDaoImpl().getStudentOrders();
    }

    public void checkOneOrder(StudentOrder so){
        AnswerCityRegister cityAnswer = checkCityRegister(so);

     //   AnswerWedding weddingAnswer = checkWedding(so);
     //   AnswerChildren childrenAnswer = checkChildren(so);
     //   AnswerStudent studentAnswer = checkStudent(so);

     //   sendMail(so);
    }


    public AnswerCityRegister checkCityRegister(StudentOrder so) {
        return cityRegisterVal.checkCityRegister(so);
    }

    public AnswerWedding checkWedding(StudentOrder so) {
        return weddingVal.checkWedding(so);
    }

    public AnswerChildren checkChildren(StudentOrder so) {
        return childrenVal.checkChildren(so);
    }

    public AnswerStudent checkStudent(StudentOrder so) {
        return studentVal.checkStudent(so);
    }

    public void sendMail(StudentOrder so){
        mailSender.sendMail(so);
    }

}
