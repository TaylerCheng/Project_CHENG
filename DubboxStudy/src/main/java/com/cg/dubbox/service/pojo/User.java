package com.cg.dubbox.service.pojo;

/**
 * Created by Cheng Guang on 2016/9/6.
 */
public class User {
    private long id;
    private String name;
    private int age;
    private String sex;

    public User() {
    }

    public User(Long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }
}
