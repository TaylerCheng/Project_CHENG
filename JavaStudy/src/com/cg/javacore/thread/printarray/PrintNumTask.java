package com.cg.javacore.thread.printarray;

/**
 * @author： Cheng Guang
 * @date： 2017/11/30.
 */
public class PrintNumTask implements Runnable {

    private NeedPrintArray arrayAndLock;
    private int currentIndex = 0;
    private int arrayLenth;

    private int[] numberArray;

    public PrintNumTask(NeedPrintArray printArray) {
        this.arrayAndLock = printArray;
        this.numberArray = printArray.numberArray;
        arrayLenth = numberArray.length;
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
                if (!arrayAndLock.printNumFlag) {
                    arrayAndLock.wait();
                }
                if (arrayAndLock.printNumFlag) {
                    if (currentIndex < arrayLenth) {
                        System.out.println(numberArray[currentIndex++]);
                    }
                    if (currentIndex < arrayLenth) {
                        System.out.println(numberArray[currentIndex++]);
                    }
                    arrayAndLock.printNumFlag = false;
                    arrayAndLock.notify();
                }

            }
        }
    }

}
