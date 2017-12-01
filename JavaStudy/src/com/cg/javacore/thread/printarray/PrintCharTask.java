package com.cg.javacore.thread.printarray;

/**
 * @author： Cheng Guang
 * @date： 2017/11/30.
 */
public class PrintCharTask implements Runnable {

    private NeedPrintArray arrayAndLock;
    private int currentIndex = 0;
    private int arrayLenth;

    private char[] charArray;

    public PrintCharTask(NeedPrintArray printArray) {
        this.arrayAndLock = printArray;
        this.charArray = printArray.charArray;
        this.arrayLenth = charArray.length;
    }

    @Override
    public void run() {
        try {
            printItem();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void printItem() throws InterruptedException {
        synchronized (arrayAndLock) {
            while (currentIndex < arrayLenth) {
                if (arrayAndLock.printNumFlag) {
                    arrayAndLock.wait();
                }
                if (!arrayAndLock.printNumFlag) {
                    if (currentIndex < arrayLenth) {
                        System.out.println(charArray[currentIndex++]);
                    }
                    arrayAndLock.printNumFlag = true;
                    arrayAndLock.notify();
                }

            }
        }
    }

}
