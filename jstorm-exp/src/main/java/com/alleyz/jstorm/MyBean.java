package com.alleyz.jstorm;

import java.io.Serializable;

/**
 * Created by zhaihw on 2017/4/25.
 *
 */
public class MyBean implements Serializable {
    private static final Long serialVersionUID = 1L;

    private String role;
    private String txt;

    public MyBean(String role, String txt) {
        this.role = role;
        this.txt = txt;
    }

    public MyBean() {
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getTxt() {
        return txt;
    }

    public void setTxt(String txt) {
        this.txt = txt;
    }
}
