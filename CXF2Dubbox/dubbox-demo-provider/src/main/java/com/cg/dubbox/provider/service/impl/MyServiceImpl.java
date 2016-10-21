package com.cg.dubbox.provider.service.impl;

import com.cg.dubbox.api.service.MyService;
import com.cg.dubbox.model.Book;
import com.cg.dubbox.model.Product;
import com.cg.dubbox.model.User;

import javax.ws.rs.Path;
import javax.xml.bind.JAXBElement;
import javax.xml.namespace.QName;

/**
 * Created by Cheng Guang on 2016/9/28.
 */
public class MyServiceImpl implements MyService {

    private static final String XMLNS_NAMESPACE = "com.cg.web.cxf.jaxrs.MyServiceImpl";

    final String ROOT_NODE = "root";

    @Override
    public JAXBElement<String> getHelloWorldJSON() {
        JAXBElement<String> result = new JAXBElement<String>(new QName("",
                ROOT_NODE), String.class, sayHelloWorld());
        return result;
    }

    @Override
    public JAXBElement<String> getHelloWorldXML() {
        JAXBElement<String> result = new JAXBElement<String>(new QName(
                XMLNS_NAMESPACE, ROOT_NODE), String.class, sayHelloWorld());
        return result;
    }

    @Override
    public JAXBElement<String> getHelloToNameJSON( String name) {
        JAXBElement<String> result = new JAXBElement<String>(new QName("",
                ROOT_NODE), String.class, sayHelloToName(name));
        return result;
    }

    @Override
    public JAXBElement<String> getHelloToNameXML(String name) {
        JAXBElement<String> result = new JAXBElement<String>(new QName(
                XMLNS_NAMESPACE, ROOT_NODE), String.class, sayHelloToName(name));
        return result;
    }

    @Override
    public User getUserInXML() {
        return getUser("unknown");
    }

    @Override
    public User getUserInJSON() {
        return getUser("unknown");
    }

    @Override
    public User getUserInXML(String username) {
        return getUser(username);
    }

    @Override
    public User getUserInJSON(String username) {
        return getUser(username);
    }

    @Override
    public JAXBElement<User> getUserInXML2(String username) {
        JAXBElement<User> result = new JAXBElement<User>(new QName(
                XMLNS_NAMESPACE, ROOT_NODE), User.class, getUser(username));
        return result;
    }

    @Override
    public JAXBElement<User> getUserInJSON2(String username) {
        JAXBElement<User> result = new JAXBElement<User>(new QName("",
                ROOT_NODE), User.class, getUser(username));
        return result;
    }

    @Override
    public Product getProductXML(Product product) {
        return getProductFromServer(product);
    }

    @Override
    public JAXBElement<Product> getProductXML2(
            Product product) {
        JAXBElement<Product> result = new JAXBElement<Product>(new QName(
                XMLNS_NAMESPACE, ROOT_NODE), Product.class,
                getProductFromServer(product));
        return result;
    }

    @Override
    public Book getBookXML(Book book) {
        return getBookFromServer(book);
    }

    @Override
    public Book getBookJSON(Book book) {
        return getBookFromServer(book);
    }

    @Override
    public Book addBookJSON(Book book) {
        return getBookFromServer(book);
    }

    @Override
    public Book addBookXML(Book book) {
        return getBookFromServer(book);
    }

    private Product getProductFromServer(Product p) {
        p.setProductName(p.getProductName() + " from server!");
        return p;
    }

    private Book getBookFromServer(Book book) {
        book.setProductName(book.getProductName() + " from server!");
        return book;
    }

    private User getUser(String username) {
        User user = new User();
        user.setUsername(username);
        String pwd = new Long(Math.round(Math.random() * 100000)).toString();
        user.setPassword(pwd);
        int pin = (int) (Math.random() * 1000);
        user.setPin(pin);
        return user;
    }

    private String sayHelloWorld() {
        return "Hello JAX-RS!";
    }

    private String sayHelloToName(String name) {
        return "Hello " + name + ",welcome to the world of JAX-RS!";
    }
}

