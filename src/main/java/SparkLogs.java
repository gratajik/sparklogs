import com.jcraft.jsch.*;

import javax.swing.*;
import java.awt.*;
import java.io.*;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SparkLogs {
    private static String[] servers = new String[]{};
    private static String sparkLogPath = "";
    private static String sparkUserName = "";
    private static String sparkPassword = "";

    public static void main(String[] arg) {
        if (arg.length < 2 || arg.length > 3) {
            System.err.println("usage  : SparkLogs jobid outputdir [password]");
            System.err.println("example: java -jar sparklogs.jar app-20140430134154-0461 . mypassword");
            System.exit(0);
        }

        if (arg.length == 3) {
            sparkPassword = arg[2];
        }

        try {
            String jobId = arg[0];
            String outputDir = arg[1];
            if (!outputDir.endsWith("/")) {
                outputDir += "/";
            }

            outputDir += jobId + "/";
            if (!getConfig()) {
                System.out.println("Unable to load resource config.properties from SparkLogs jar");
                System.exit(-2);
            }

            File dir = new File(outputDir);
            if (!dir.exists() || !dir.isDirectory()) {
                dir.mkdir();
            }

            for (String server : servers) {
                System.out.println("Loading from server " + server);
                JSch jsch = new JSch();
                Session session = jsch.getSession(sparkUserName, server, 22);

                if (sparkPassword.length() > 0) {
                    session.setPassword(sparkPassword);
                }

                UserInfo ui = new MyUserInfo();
                session.setUserInfo(ui);
                session.connect();

                String[] files = findFiles(session, sparkLogPath + jobId);
                if (files == null) {
                    System.out.println("\tNo files to process");
                } else {
                    for (String file : files) {
                        String outputFile = server + "-" + flattenFilename(file);
                        copyFile(session, file, outputDir + outputFile);
                    }
                }

                session.disconnect();
            }

            System.exit(0);
        } catch (Exception e) {
            System.out.println(e);
            System.exit(-1);
        }
    }

    protected static String flattenFilename(String path) {
        Pattern patternPath = Pattern.compile("app-(.*)");
        Matcher matcherPath = patternPath.matcher(path);

        if (matcherPath.find()) {
            return matcherPath.group(0).replace("/", "-");
        } else {
            return "";
        }
    }

    static boolean getConfig() {
        try {
            Properties configFile = new Properties();
            configFile.load(SparkLogs.class.getClassLoader().getResourceAsStream("config.properties"));
            servers = configFile.getProperty("sparkServers").split(",");
            sparkLogPath = configFile.getProperty("sparkLogPath");
            sparkUserName = configFile.getProperty("sparkUserId");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    static String[] findFiles(Session session, String path) throws JSchException, IOException {
        try {
            String command = "find  " + path + " -name 'stderr' -o -name 'stdout' -o -name 'maana.log'";
            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);

            InputStream in = channel.getInputStream();
            channel.connect();

            String inputStreamString = new Scanner(in, "UTF-8").useDelimiter("\\A").next();
            return inputStreamString.split("\n");
        } catch (Exception e) {
            return null;
        }
    }

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

    public static class MyUserInfo implements UserInfo, UIKeyboardInteractive {
        public String getPassword() {
            return password;
        }

        // Override this - always answer yes.  This will assume trust on the target server
        public boolean promptYesNo(String str) {
            return true;
        }

        String password;
        JTextField passwordField = (JTextField) new JPasswordField(20);

        public String getPassphrase() {
            return null;
        }

        public boolean promptPassphrase(String message) {
            return true;
        }

        public boolean promptPassword(String message) {
            Object[] ob = {passwordField};
            int result = JOptionPane.showConfirmDialog(null, ob, message,
                    JOptionPane.OK_CANCEL_OPTION);
            if (result == JOptionPane.OK_OPTION) {
                password = passwordField.getText();
                return true;
            } else {
                return false;
            }
        }

        public void showMessage(String message) {
            JOptionPane.showMessageDialog(null, message);
        }

        final GridBagConstraints gbc =
                new GridBagConstraints(0, 0, 1, 1, 1, 1,
                        GridBagConstraints.NORTHWEST,
                        GridBagConstraints.NONE,
                        new Insets(0, 0, 0, 0), 0, 0);
        private Container panel;

        public String[] promptKeyboardInteractive(String destination,
                                                  String name,
                                                  String instruction,
                                                  String[] prompt,
                                                  boolean[] echo) {
            panel = new JPanel();
            panel.setLayout(new GridBagLayout());

            gbc.weightx = 1.0;
            gbc.gridwidth = GridBagConstraints.REMAINDER;
            gbc.gridx = 0;
            panel.add(new JLabel(instruction), gbc);
            gbc.gridy++;

            gbc.gridwidth = GridBagConstraints.RELATIVE;

            JTextField[] texts = new JTextField[prompt.length];
            for (int i = 0; i < prompt.length; i++) {
                gbc.fill = GridBagConstraints.NONE;
                gbc.gridx = 0;
                gbc.weightx = 1;
                panel.add(new JLabel(prompt[i]), gbc);

                gbc.gridx = 1;
                gbc.fill = GridBagConstraints.HORIZONTAL;
                gbc.weighty = 1;
                if (echo[i]) {
                    texts[i] = new JTextField(20);
                } else {
                    texts[i] = new JPasswordField(20);
                }
                panel.add(texts[i], gbc);
                gbc.gridy++;
            }

            if (JOptionPane.showConfirmDialog(null, panel,
                    destination + ": " + name,
                    JOptionPane.OK_CANCEL_OPTION,
                    JOptionPane.QUESTION_MESSAGE)
                    == JOptionPane.OK_OPTION) {
                String[] response = new String[prompt.length];
                for (int i = 0; i < prompt.length; i++) {
                    response[i] = texts[i].getText();
                }
                return response;
            } else {
                return null;  // cancel
            }
        }
    }
}
