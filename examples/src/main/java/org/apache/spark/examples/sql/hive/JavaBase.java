package org.apache.spark.examples.sql.hive;

/**
 * create with org.apache.spark.examples.sql.hive
 * USER: husterfox
 */
public class JavaBase {
    static int a = 1;
    int b = 0;
    public static void staticBase(String msg){
        System.out.println("I am static Base: " + msg);
    }
    public JavaBase(){
        b = 1;
        System.out.println("you have construct JavaBase");
    }
    private int ff = 1;
    private void base(){
        System.out.println("invoke base()");
    }
}