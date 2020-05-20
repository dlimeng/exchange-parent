package com.knowlegene.parent.config.model.sql;

import com.knowlegene.parent.config.common.constantenum.TokenEnum;

/**
 * @Author: limeng
 * @Date: 2019/7/31 14:44
 */
public class Lexer {
    private TokenEnum token;
    private Lexer next;
    private Lexer pre;

    public TokenEnum getToken() {
        return token;
    }

    public void setToken(TokenEnum token) {
        this.token = token;
    }

    public Lexer getNext() {
        return next;
    }

    public void setNext(Lexer next) {
        this.next = next;
    }

    public Lexer getPre() {
        return pre;
    }

    public void setPre(Lexer pre) {
        this.pre = pre;
    }
}
