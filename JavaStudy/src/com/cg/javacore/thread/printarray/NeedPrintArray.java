package com.cg.javacore.thread.printarray;

/**
 * @author： Cheng Guang
 * @date： 2017/12/1.
 */
public class NeedPrintArray {

    boolean printNumFlag =true;


    public int[] numberArray = new int[10];
    {
        for (int i = 1; i <= 10; i++) {
            numberArray[i-1] = i;
        }
    }

    public char[] charArray = {'a','b','c','d','e'};

}
