package com.cg.mapreduce.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Windows开发环境提交作业到Yarn集群工具类
 * <p>
 * Created by Cheng Guang on 2016/9/14.
 */
public class YarnJobUtil {

    private static final Logger logger = Logger.getLogger(YarnJobUtil.class);

    private static File jarFile = null;

    public static File getJobJarFile(String classpath) throws IOException {
        if (jarFile == null) {
            jarFile = createTempJar(classpath);
        }
        return jarFile;
    }

    private static File createTempJar(String classpath) throws IOException {
        if (!new File(classpath).exists()) {
            return null;
        }
        final File tempJarFile = File.createTempFile("YarnJob-", ".jar", new File(System.getProperty("java.io.tmpdir")));
        JarOutputStream out = null;
        try {
            Manifest manifest = new Manifest();
            manifest.getMainAttributes().putValue("Manifest-Version", "1.0");

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    tempJarFile.delete();
                }
            });
            out = new JarOutputStream(
                    new FileOutputStream(tempJarFile), manifest);
            createTempJarInner(out, new File(classpath), "");
        } catch (IOException e) {
            logger.info("Init job on yarn failed.", e);
            throw e;
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {

                }
            }
        }
        return tempJarFile;
    }

    private static void createTempJarInner(JarOutputStream out, File f,
            String base) throws IOException {
        if (f.isDirectory()) {
            File[] fl = f.listFiles();
            if (base.length() > 0) {
                base = base + "/";
            }
            for (int i = 0; i < fl.length; i++) {
                createTempJarInner(out, fl[i], base + fl[i].getName());
            }
        } else {
            out.putNextEntry(new JarEntry(base));
            FileInputStream in = new FileInputStream(f);
            byte[] buffer = new byte[1024];
            int n = in.read(buffer);
            while (n != -1) {
                out.write(buffer, 0, n);
                n = in.read(buffer);
            }
            in.close();
        }
    }

}
