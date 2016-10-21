package com.cg.dubbox.api.service;


import com.cg.dubbox.model.Product;
import com.cg.dubbox.model.Book;
import com.cg.dubbox.model.User;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBElement;

/**
 * Created by Cheng Guang on 2016/9/28.
 */
@Path("/myservice")
public interface MyService {

    @GET
    @Path("/json/hello")
    @Produces(MediaType.APPLICATION_JSON)
    public JAXBElement<String> getHelloWorldJSON();

    @GET
    @Path("/xml/hello")
    @Produces(MediaType.APPLICATION_XML)
    public JAXBElement<String> getHelloWorldXML();

    @GET
    @Path("/json/hi/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public JAXBElement<String> getHelloToNameJSON(@PathParam("name") String name);

    @GET
    @Path("/xml/hi/{name}")
    @Produces(MediaType.APPLICATION_XML)
    public JAXBElement<String> getHelloToNameXML(@PathParam("name") String name);

    @GET
    @Path("/xml/user")
    @Produces(MediaType.APPLICATION_XML)
    public User getUserInXML();

    @GET
    @Path("/json/user")
    @Produces(MediaType.APPLICATION_JSON)
    public User getUserInJSON();

    @GET
    @Path("/xml/userByName/{name}")
    @Produces(MediaType.APPLICATION_XML)
    public User getUserInXML(@PathParam("name") String username);

    @GET
    @Path("/json/userByName/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public User getUserInJSON(@PathParam("name") String username);

    @GET
    @Path("/xml/userByName2/{name}")
    @Produces(MediaType.APPLICATION_XML)
    public JAXBElement<User> getUserInXML2(@PathParam("name") String username);

    @GET
    @Path("/json/userByName2/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public JAXBElement<User> getUserInJSON2(@PathParam("name") String username);

    @GET
    @Path("/xml/product/{name}")
    @Produces(MediaType.APPLICATION_XML)
    public Product getProductXML(@PathParam("name") Product product);

    @GET
    @Path("/xml/product2/{name}")
    @Produces(MediaType.APPLICATION_XML)
    public JAXBElement<Product> getProductXML2(
            @PathParam("name") Product product);

    @GET
    @Path("/xml/book/{ISBN}")
    @Produces(MediaType.APPLICATION_XML)
    public Book getBookXML(@PathParam("ISBN") Book book);

    @GET
    @Path("/json/book/{ISBN}")
    @Produces(MediaType.APPLICATION_JSON)
    public Book getBookJSON(@PathParam("ISBN") Book book);

    @POST
    @Path("/json/add-book")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Book addBookJSON(Book book);

    @POST
    @Path("/xml/add-book")
    @Produces(MediaType.APPLICATION_XML)
    @Consumes(MediaType.APPLICATION_XML)
    public Book addBookXML(Book book);

}

