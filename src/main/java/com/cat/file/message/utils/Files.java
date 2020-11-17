package com.cat.file.message.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Files {
    public Files() {
    }

    public static Files.Dir forDir() {
        return Files.Dir.INSTANCE;
    }

    public static Files.IO forIO() {
        return Files.IO.INSTANCE;
    }

    public static Files.Zip forZip() {
        return Files.Zip.INSTANCE;
    }

    public static enum Zip {
        INSTANCE;

        private Zip() {
        }

        public List<String> copyDir(ZipInputStream zis, File baseDir) throws IOException {
            return this.copyDir(zis, baseDir, (Files.Policy)null);
        }

        public List<String> copyDir(ZipInputStream zis, File baseDir, Files.Policy policy) throws IOException {
            List<String> entryNames = new ArrayList();
            if (!baseDir.exists()) {
                Files.Dir.INSTANCE.createDir(baseDir);
            }

            while(true) {
                ZipEntry entry;
                do {
                    entry = zis.getNextEntry();
                    if (entry == null) {
                        return entryNames;
                    }
                } while(policy != null && !policy.apply(entry.getName()));

                if (entry.isDirectory()) {
                    Files.Dir.INSTANCE.createDir(new File(baseDir, entry.getName()));
                } else {
                    File target = new File(baseDir, entry.getName());
                    target.getParentFile().mkdirs();
                    Files.IO.INSTANCE.copy(zis, new FileOutputStream(target), Files.AutoClose.OUTPUT);
                }
            }
        }
    }

    public interface Policy {
        boolean apply(String var1);
    }

    public static enum IO {
        INSTANCE;

        private IO() {
        }

        public void copy(InputStream is, OutputStream os) throws IOException {
            this.copy(is, os, Files.AutoClose.NONE);
        }

        public void copy(InputStream is, OutputStream os, Files.AutoClose stream) throws IOException {
            byte[] content = new byte[4096];

            try {
                while(true) {
                    int size = is.read(content);
                    if (size == -1) {
                        return;
                    }

                    os.write(content, 0, size);
                }
            } finally {
                stream.close(is);
                stream.close(os);
            }
        }

        public byte[] readFrom(File file) throws IOException {
            return this.readFrom(new FileInputStream(file), (int)file.length());
        }

        public String readFrom(File file, String charsetName) throws IOException {
            byte[] content = this.readFrom(new FileInputStream(file), (int)file.length());
            return new String(content, charsetName);
        }

        public byte[] readFrom(InputStream is) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(16384);
            this.copy(is, baos, Files.AutoClose.INPUT);
            return baos.toByteArray();
        }

        public byte[] readFrom(InputStream is, int expectedSize) throws IOException {
            byte[] content = new byte[expectedSize];

            int size;
            try {
                for(int count = 0; count < expectedSize; count += size) {
                    size = is.read(content, count, expectedSize - count);
                    if (size == -1) {
                        break;
                    }
                }
            } finally {
                try {
                    is.close();
                } catch (IOException var11) {
                    ;
                }

            }

            return content;
        }

        public String readFrom(InputStream is, String charsetName) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(16384);
            this.copy(is, baos, Files.AutoClose.INPUT);
            return baos.toString(charsetName);
        }

        public void writeTo(File file, byte[] data) throws IOException {
            if (file.isDirectory()) {
                throw new IOException(String.format("Can't write to an existing directory(%s)", file));
            } else {
                Files.Dir.INSTANCE.createDir(file.getParentFile());
                FileOutputStream fos = new FileOutputStream(file);

                try {
                    fos.write(data);
                } finally {
                    try {
                        fos.close();
                    } catch (IOException var10) {
                        ;
                    }

                }

            }
        }

        public void writeTo(File file, String data) throws IOException {
            this.writeTo(file, data, "utf-8");
        }

        public void writeTo(File file, String data, String charsetName) throws IOException {
            this.writeTo(file, data.getBytes(charsetName));
        }
    }

    public static enum Dir {
        INSTANCE;

        private Dir() {
        }

        public void copyDir(File from, File to) throws IOException {
            this.copyDir(from, to, (Files.Policy)null);
        }

        public void copyDir(File from, File to, Files.Policy policy) throws IOException {
            String[] names = from.list();
            this.createDir(to);
            if (names != null) {
                String[] arr$ = names;
                int len$ = names.length;

                for(int i$ = 0; i$ < len$; ++i$) {
                    String name = arr$[i$];
                    File file = new File(from, name);
                    if (policy == null || policy.apply(file.getPath())) {
                        if (file.isDirectory()) {
                            this.copyDir(file, new File(to, name));
                        } else {
                            this.copyFile(file, new File(to, name));
                        }
                    }
                }
            }

        }

        public void copyFile(File from, File to) throws IOException {
            this.createDir(to.getParentFile());
            Files.IO.INSTANCE.copy(new FileInputStream(from), new FileOutputStream(to), Files.AutoClose.INPUT_OUTPUT);
            to.setLastModified(from.lastModified());
        }

        public void createDir(File dir) {
            if (!dir.exists() && !dir.mkdirs()) {
                throw new RuntimeException(String.format("Cant' create directory(%s)!", dir));
            }
        }

        public boolean delete(File file) {
            return this.delete(file, false);
        }

        public boolean delete(File file, boolean recursive) {
            if (file.exists()) {
                if (file.isFile()) {
                    return file.delete();
                }

                if (file.isDirectory()) {
                    if (recursive) {
                        File[] children = file.listFiles();
                        if (children != null) {
                            File[] arr$ = children;
                            int len$ = children.length;

                            for(int i$ = 0; i$ < len$; ++i$) {
                                File child = arr$[i$];
                                this.delete(child, recursive);
                            }
                        }
                    }

                    return file.delete();
                }
            }

            return false;
        }
    }

    public static enum AutoClose {
        NONE,
        INPUT,
        OUTPUT,
        INPUT_OUTPUT;

        private AutoClose() {
        }

        public void close(InputStream is) {
            if ((this == INPUT || this == INPUT_OUTPUT) && is != null) {
                try {
                    is.close();
                } catch (IOException var3) {
                    ;
                }
            }

        }

        public void close(OutputStream os) {
            if ((this == OUTPUT || this == INPUT_OUTPUT) && os != null) {
                try {
                    os.close();
                } catch (IOException var3) {
                    ;
                }
            }

        }
    }
}
