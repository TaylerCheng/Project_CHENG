package com.cg.javacore.innerclass;

/**
 * @author： Cheng Guang
 * @date： 2017/4/12.
 */
public class OuterClass {
    private int o;

    public class InnerClass {
        private int i = 0;
        public int getOuter() {
            //可以访问外部类对象的私有成员
             return OuterClass.this.o;
        }
    }

    public int getInner(InnerClass inner){
        return inner.i;
    }

    public static void main(String[] args) {
        OuterClass outerClass = new OuterClass();
        InnerClass innerClass = outerClass.new InnerClass();

        OuterClass outerClass2 = new OuterClass();
        InnerClass innerClass2 = outerClass2.new InnerClass();

        System.out.println(outerClass.getInner(innerClass2));
    }

}
