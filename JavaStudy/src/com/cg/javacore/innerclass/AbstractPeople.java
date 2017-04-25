package com.cg.javacore.innerclass;

import java.io.File;

/**
 * @author： Cheng Guang
 * @date： 2017/4/13.
 */
public abstract class AbstractPeople {
    public String fileName;
    public File file = new File(fileName);

    public boolean exists() {
        return file.exists();
    }

}
