package com.cg.javacore.thread.printarray;


/**
 * @author： Cheng Guang
 * @date： 2017/11/30.
 */
public class PrintTest {

    public static void main(String[] args) throws InterruptedException {
        NeedPrintArray needPrintArray = new NeedPrintArray();

        PrintCharTask printCharTask = new PrintCharTask(needPrintArray);
        PrintNumTask printNumTask = new PrintNumTask(needPrintArray);

        Thread thread1 = new Thread(printCharTask);
        thread1.start();

        Thread thread2 = new Thread(printNumTask);
        thread2.start();
    }

}

