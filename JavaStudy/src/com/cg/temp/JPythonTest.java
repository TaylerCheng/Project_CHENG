package com.cg.temp;

import org.python.core.PyFunction;
import org.python.core.PyInteger;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.util.PythonInterpreter;

/**
 * @author： Cheng Guang
 * @date： 2017/8/31.
 */
public class JPythonTest {

    public static void main(String[] args) {
        PythonInterpreter interpreter = new PythonInterpreter();
        interpreter.execfile("C:\\Users\\Administrator\\Desktop\\printtest.py");
        PyFunction func = interpreter.get("printWord", PyFunction.class);

        PyObject pyobj = func.__call__(new PyString("cheng"));
        System.out.println(pyobj.asString());
        System.out.println(pyobj.toString());
    }
}
