import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HdfsOperations {
    private static FileSystem fs;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            return;
        }

        Configuration conf = new Configuration();
        fs = FileSystem.get(conf);

        try {
            switch (args[0]) {
                case "upload":
                    require(args, 4);
                    upload(args[1], args[2], args[3]);
                    break;
                case "download":
                    require(args, 3);
                    download(args[1], args[2]);
                    break;
                case "cat":
                    require(args, 2);
                    cat(args[1]);
                    break;
                case "info":
                    require(args, 2);
                    info(args[1]);
                    break;
                case "lsr":
                    require(args, 2);
                    listRecursive(new Path(args[1]));
                    break;
                case "touch":
                    require(args, 2);
                    touch(args[1]);
                    break;
                case "deleteFile":
                    require(args, 2);
                    deleteFile(args[1]);
                    break;
                case "mkdir":
                    require(args, 2);
                    mkdir(args[1]);
                    break;
                case "rmdir":
                    require(args, 2);
                    rmdir(args[1]);
                    break;
                case "append":
                    require(args, 4);
                    append(args[1], args[2], args[3]);
                    break;
                case "move":
                    require(args, 3);
                    move(args[1], args[2]);
                    break;
                default:
                    usage();
            }
        } finally {
            fs.close();
        }
    }

    private static void upload(String localFile, String hdfsFile, String mode) throws Exception {
        Path local = new Path(localFile);
        Path target = new Path(hdfsFile);
        if (target.getParent() != null) {
            fs.mkdirs(target.getParent());
        }

        if (!fs.exists(target)) {
            fs.copyFromLocalFile(false, false, local, target);
            System.out.println("上传完成：" + target);
            return;
        }

        if ("append".equalsIgnoreCase(mode)) {
            try (InputStream in = new FileInputStream(localFile);
                 FSDataOutputStream out = fs.append(target)) {
                IOUtils.copyBytes(in, out, 4096, false);
            }
            System.out.println("已追加到：" + target);
        } else if ("overwrite".equalsIgnoreCase(mode)) {
            fs.copyFromLocalFile(false, true, local, target);
            System.out.println("已覆盖：" + target);
        } else {
            throw new IllegalArgumentException("mode只能是append或overwrite");
        }
    }

    private static void download(String hdfsFile, String localDirectory) throws Exception {
        Path source = new Path(hdfsFile);
        if (!fs.exists(source)) {
            throw new IllegalArgumentException("HDFS文件不存在：" + source);
        }

        File directory = new File(localDirectory);
        if (!directory.exists() && !directory.mkdirs()) {
            throw new IllegalStateException("无法创建本地目录：" + directory);
        }

        String name = source.getName();
        File output = new File(directory, name);
        if (output.exists()) {
            String stamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
            int dot = name.lastIndexOf('.');
            String renamed = dot > 0
                    ? name.substring(0, dot) + "_" + stamp + name.substring(dot)
                    : name + "_" + stamp;
            output = new File(directory, renamed);
        }

        fs.copyToLocalFile(false, source, new Path(output.getAbsolutePath()), true);
        System.out.println("下载完成：" + output.getAbsolutePath());
    }

    private static void cat(String hdfsFile) throws Exception {
        try (FSDataInputStream in = fs.open(new Path(hdfsFile))) {
            IOUtils.copyBytes(in, System.out, 4096, false);
        }
    }

    private static void info(String hdfsFile) throws Exception {
        FileStatus status = fs.getFileStatus(new Path(hdfsFile));
        System.out.println("权限：" + status.getPermission());
        System.out.println("大小：" + status.getLen() + " bytes");
        System.out.println("修改时间：" + new Date(status.getModificationTime()));
        System.out.println("路径：" + status.getPath());
    }

    private static void listRecursive(Path path) throws Exception {
        FileStatus status = fs.getFileStatus(path);
        printStatus(status);
        if (status.isDirectory()) {
            for (FileStatus child : fs.listStatus(path)) {
                listRecursive(child.getPath());
            }
        }
    }

    private static void printStatus(FileStatus status) {
        System.out.printf("%s\t%d\t%s\t%s%n",
                status.getPermission(),
                status.getLen(),
                new Date(status.getModificationTime()),
                status.getPath());
    }

    private static void touch(String hdfsFile) throws Exception {
        Path file = new Path(hdfsFile);
        if (file.getParent() != null) {
            fs.mkdirs(file.getParent());
        }
        if (fs.exists(file)) {
            System.out.println("文件已经存在：" + file);
        } else {
            fs.createNewFile(file);
            System.out.println("文件创建成功：" + file);
        }
    }

    private static void deleteFile(String hdfsFile) throws Exception {
        Path file = new Path(hdfsFile);
        System.out.println(fs.delete(file, false) ? "文件删除成功" : "文件不存在或删除失败");
    }

    private static void mkdir(String hdfsDirectory) throws Exception {
        Path directory = new Path(hdfsDirectory);
        System.out.println(fs.mkdirs(directory) ? "目录创建成功" : "目录已存在或创建失败");
    }

    private static void rmdir(String hdfsDirectory) throws Exception {
        Path directory = new Path(hdfsDirectory);
        if (!fs.exists(directory)) {
            System.out.println("目录不存在");
        } else if (!fs.getFileStatus(directory).isDirectory()) {
            System.out.println("指定路径不是目录");
        } else if (fs.listStatus(directory).length != 0) {
            System.out.println("目录非空，不执行删除");
        } else {
            System.out.println(fs.delete(directory, false) ? "空目录删除成功" : "目录删除失败");
        }
    }

    private static void append(String hdfsFile, String position, String content) throws Exception {
        Path file = new Path(hdfsFile);
        byte[] added = (content + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);

        if ("tail".equalsIgnoreCase(position)) {
            try (FSDataOutputStream out = fs.append(file)) {
                out.write(added);
            }
            System.out.println("内容已追加到文件末尾");
        } else if ("head".equalsIgnoreCase(position)) {
            byte[] old;
            try (FSDataInputStream in = fs.open(file);
                 ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                IOUtils.copyBytes(in, buffer, 4096, false);
                old = buffer.toByteArray();
            }
            try (FSDataOutputStream out = fs.create(file, true)) {
                out.write(added);
                out.write(old);
            }
            System.out.println("内容已添加到文件开头");
        } else {
            throw new IllegalArgumentException("position只能是head或tail");
        }
    }

    private static void move(String source, String destination) throws Exception {
        Path src = new Path(source);
        Path dst = new Path(destination);
        if (dst.getParent() != null) {
            fs.mkdirs(dst.getParent());
        }
        System.out.println(fs.rename(src, dst) ? "移动成功" : "移动失败");
    }

    private static void require(String[] args, int count) {
        if (args.length < count) {
            usage();
            throw new IllegalArgumentException("参数不足");
        }
    }

    private static void usage() {
        System.out.println("用法：");
        System.out.println("  upload <本地文件> <HDFS文件> <append|overwrite>");
        System.out.println("  download <HDFS文件> <本地目录>");
        System.out.println("  cat <HDFS文件>");
        System.out.println("  info <HDFS文件>");
        System.out.println("  lsr <HDFS目录>");
        System.out.println("  touch <HDFS文件>");
        System.out.println("  deleteFile <HDFS文件>");
        System.out.println("  mkdir <HDFS目录>");
        System.out.println("  rmdir <HDFS目录>");
        System.out.println("  append <HDFS文件> <head|tail> <内容>");
        System.out.println("  move <HDFS源路径> <HDFS目标路径>");
    }
}
