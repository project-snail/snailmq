package com.snail.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

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
                "FileUtil#writeToFileWithBackup 存储位置不存在，自动创建目录 {} {}",
                dirPathFile, (mkdirsResult ? "OK" : "FAILED")
            );
            return new File[0];
        }
        return dirPathFile.listFiles();
    }

    public static File[] getDirFilesWithCreateDir(String dirPath) {
        return getDirFilesWithCreateDir(new File(dirPath));
    }

    public static void writeToFileWithBackup(String content, String filePath) {

        File file = new File(filePath);

        if (!file.exists()) {
            if (!file.getParentFile().exists()) {
                boolean mkdirs = file.getParentFile().mkdirs();
                log.info("FileUtil#writeToFileWithBackup 创建写入文件夹{}", mkdirs ? "OK" : "FAILED");
            }
            try {
                boolean newFile = file.createNewFile();
                log.warn("FileUtil#writeToFileWithBackup 创建新备份文件失败{}", newFile ? "OK" : "FAILED");
            } catch (IOException e) {
                log.error("FileUtil#writeToFileWithBackup 创建新写入文件失败", e);
            }
            return;
        }


        String oldFileContent = readFileToStr(filePath);

        if (StringUtils.isNotBlank(oldFileContent)) {
            File backFile = new File(filePath + ".bak");
            try {
                new FileOutputStream(backFile).getChannel().write(
                    ByteBuffer.wrap(
                        oldFileContent.getBytes(StandardCharsets.UTF_8)
                    )
                );
            } catch (IOException e) {
                log.error("FileUtil#writeToFileWithBackup 写入新备份文件失败{}", e);
            }
        }

        try {
            IOUtils.write(content, new FileOutputStream(file), StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.error("FileUtil#writeToFileWithBackup 写入文件失败{}", e);
        }


    }

    public static String readFileToStr(String filePath) {

        File file = new File(filePath);

        if (!file.exists()) {
            return null;
        }

        try {
            return IOUtils.toString(new FileInputStream(file), StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.info(String.format("FileUtil#readFileToStr 读取 %s 文件失败", filePath), e);
            return null;
        }

    }

    public static String readFileWithBackupWhenError(String filePath) throws IOException {

        File file = new File(filePath);

        try {
            return IOUtils.toString(new FileInputStream(file), StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.error("FileUtil#readFileWithBackupWhenError 读取文件失败，尝试读取备份文件");
            return IOUtils.toString(new FileInputStream(new File(filePath + ".bak")));
        }

    }

}
