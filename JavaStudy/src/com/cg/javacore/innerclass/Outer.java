package com.cg.javacore.innerclass;

/**
 * @author： Cheng Guang
 * @date： 2017/4/12.
 */
public class Outer {
    private int o;

    public static class Inner {
        private static final Inner INSTANCE = new Inner();

        private int i;

        private Inner() {
        }

        public void getOuter() {
            //可以访问外部类对象的私有成员
            int o = new Outer().o;
        }
    }

    public static void getInner() {
        //可以访问内部类对象的私有成员
        int i = Inner.INSTANCE.i;
    }

}
