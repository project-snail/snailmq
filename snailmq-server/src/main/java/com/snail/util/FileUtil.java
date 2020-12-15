package com.snail.util;

import lombok.extern.slf4j.Slf4j;

import java.io.File;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.util
 * @Description:
 * @date: 2020/12/15
 */
@Slf4j
public enum FileUtil {
    ;

    public static File[] getDirFilesWithCreateDir(File dirPathFile) {
        if (dirPathFile.exists() && !dirPathFile.isDirectory()) {
//            TODO 自定义Ex
            throw new RuntimeException("存储文件位置错误");
        }
        if (!dirPathFile.exists()) {
            boolean mkdirsResult = dirPathFile.mkdirs();
            log.info(
                "存储位置不存在，自动创建目录 %s %s", dirPathFile, (mkdirsResult ? "OK" : "FAILED")
            );
            return new File[0];
        }
        return dirPathFile.listFiles();
    }

    public static File[] getDirFilesWithCreateDir(String dirPath) {
        return getDirFilesWithCreateDir(new File(dirPath));
    }
}
