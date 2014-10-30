import com.jcraft.jsch.*;
import java.io.*;

public class SparkLogsUtils {
    static void copyFile(Session session, String serverFile, String localFile) throws JSchException, IOException {
        try {
            FileOutputStream fos = null;
            String command = "scp -f " + serverFile;
            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);

            OutputStream out = channel.getOutputStream();
            InputStream in = channel.getInputStream();

            channel.connect();

            byte[] buf = new byte[1024];

            // send '\0'
            buf[0] = 0;
            out.write(buf, 0, 1);
            out.flush();

            while (true) {
                int c = checkAck(in);
                if (c != 'C') {
                    break;
                }

                // read '0644 '
                in.read(buf, 0, 5);

                long filesize = 0L;
                while (true) {
                    if (in.read(buf, 0, 1) < 0) {
                        // error
                        break;
                    }
                    if (buf[0] == ' ') break;
                    filesize = filesize * 10L + (long) (buf[0] - '0');
                }

                String file = null;
                for (int i = 0; ; i++) {
                    in.read(buf, i, 1);
                    if (buf[i] == (byte) 0x0a) {
                        file = new String(buf, 0, i);
                        break;
                    }
                }

                if (filesize != 0) {
                    System.out.println("\tCopied log file to " + localFile + " (" + filesize + " bytes)");

                    // send '\0'
                    buf[0] = 0;
                    out.write(buf, 0, 1);
                    out.flush();

                    fos = new FileOutputStream(localFile);
                    int foo;
                    while (true) {
                        if (buf.length < filesize) foo = buf.length;
                        else foo = (int) filesize;
                        foo = in.read(buf, 0, foo);
                        if (foo < 0) {
                            // error
                            break;
                        }
                        fos.write(buf, 0, foo);
                        filesize -= foo;
                        if (filesize == 0L) break;
                    }
                    fos.close();
                    fos = null;

                    if (checkAck(in) != 0) {
                        System.exit(0);
                    }
                }
                // send '\0'
                buf[0] = 0;
                out.write(buf, 0, 1);
                out.flush();

            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    static int checkAck(InputStream in) throws IOException {
        int b = in.read();
        // b may be 0 for success,
        //          1 for error,
        //          2 for fatal error,
        //          -1
        if (b == 0) return b;
        if (b == -1) return b;

        if (b == 1 || b == 2) {
            StringBuffer sb = new StringBuffer();
            int c;
            do {
                c = in.read();
                sb.append((char) c);
            }
            while (c != '\n');
            if (b == 1) { // error
                System.out.print(sb.toString());
            }
            if (b == 2) { // fatal error
                System.out.print(sb.toString());
            }
        }
        return b;
    }
}
