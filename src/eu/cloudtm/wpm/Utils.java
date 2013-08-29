package eu.cloudtm.wpm;

import org.apache.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.util.Properties;

/**
 * @author Pedro Ruivo
 * @since 1.0
 */
public class Utils {

    public static final int FILE_SYSTEM_BUFFER_SIZE = 8 * 1024; //usually, filesystem has 4K or 8K block size.
    private final static Logger log = Logger.getLogger(Utils.class);

    /**
     * @return the properties read from {@param filePath}. The properties are empty if some error occurs while loading
     */
    public static Properties loadProperties(String filePath) {
        FileInputStream inputStream = null;
        Properties properties = new Properties();
        try {
            inputStream = new FileInputStream(filePath);
            properties.load(inputStream);
        } catch (FileNotFoundException e) {
            log.error("Trying to load properties from a non-existing file: " + filePath, e);
        } catch (IOException e) {
            log.error("Error loading properties from: " + filePath, e);
        } finally {
            safeClose(inputStream);
        }
        return properties;
    }

    /**
     * Close a {@link Closeable} ignoring possible exceptions.
     */
    public static void safeClose(Closeable closeable) {
        if (closeable == null) {
            //nothing to close
            return;
        }
        try {
            closeable.close();
        } catch (IOException e) {
            //suppressed
        }
    }

    /**
     * Close a {@link Socket} ignoring possible exceptions.
     */
    public static void safeClose(Socket socket) {
        if (socket == null) {
            //nothing to close
            return;
        }
        try {
            socket.close();
        } catch (IOException e) {
            //suppressed
        }
    }

    /**
     * Same as {@link #safeClose(java.io.Closeable)}
     */
    public static void safeCloseAll(Closeable... closeables) {
        if (closeables == null || closeables.length == 0) {
            return;
        } else if (closeables.length == 1) {
            safeClose(closeables[0]);
            return;
        }
        for (Closeable closeable : closeables) {
            safeClose(closeable);
        }
    }

    /**
     * Copies one file to another
     */
    public static void copyFile(File src, File dst) throws IOException {
        InputStream in = null;
        OutputStream out = null;
        final boolean isDebugEnabled = log.isDebugEnabled();
        try {
            if (isDebugEnabled) {
                log.debug("Copying " + src.getAbsolutePath() + " to " + dst.getAbsolutePath());
            }
            in = new FileInputStream(src);
            //For Overwrite the file.
            out = new FileOutputStream(dst);
            int transferredBytes = 0;
            byte[] buf = new byte[FILE_SYSTEM_BUFFER_SIZE];
            int len;
            while ((len = in.read(buf)) > 0) {
                out.write(buf, 0, len);
                if (isDebugEnabled) {
                    transferredBytes += len;
                }
            }
            if (isDebugEnabled) {
                log.debug("Copied " + src.getAbsolutePath() + " to " + dst.getAbsolutePath() + " (" + transferredBytes + " bytes)");
            }
        } catch (IOException e) {
            log.error("Error copying " + src.getAbsolutePath() + " to " + dst.getAbsolutePath(), e);
            throw e;
        } finally {
            Utils.safeCloseAll(in, out);
        }
    }

    /**
     * Sends the file to a {@link DataOutputStream}
     */
    public static void sendFile(File file, DataOutputStream dos) throws IOException {
        InputStream is = null;
        final boolean isDebugEnabled = log.isDebugEnabled();
        try {
            is = new FileInputStream(file);
            final byte[] buffer = new byte[FILE_SYSTEM_BUFFER_SIZE];
            if (isDebugEnabled) {
                log.debug("Sending " + file.getAbsolutePath() + " ...");
            }
            dos.writeUTF(file.getName());
            dos.writeLong(file.length());
            int read;
            while ((read = is.read(buffer)) > 0) {
                dos.write(buffer, 0, read);
            }
            dos.flush();
            if (isDebugEnabled) {
                log.debug("File " + file.getAbsolutePath() + " sent!");
            }
        } catch (IOException e) {
            log.error("Error sending " + file.getAbsolutePath(), e);
            throw e;
        } finally {
            Utils.safeClose(is);
        }
    }

    public static File receiveFile(DataInputStream dis) throws IOException {
        OutputStream os = null;
        final boolean isDebugEnabled = log.isDebugEnabled();
        try {
            if (isDebugEnabled) {
                log.debug("Receiving file...");
            }
            final byte[] buffer = new byte[FILE_SYSTEM_BUFFER_SIZE];
            final String filename = dis.readUTF();
            File file = new File("log/ack/" + filename);
            if (isDebugEnabled) {
                log.debug("Receiving file... Store to " + file.getAbsolutePath());
            }
            os = new FileOutputStream(file);
            long size = dis.readLong();
            int read;
            while (size > 0 && (read = dis.read(buffer)) > -1) {
                os.write(buffer, 0, read);
                size -= read;
            }
            os.flush();
            if (isDebugEnabled) {
                log.debug("File " + file.getAbsolutePath() + " received!");
            }
            return file;
        } catch (IOException e) {
            log.error("Error receiving file...", e);
            throw e;
        } finally {
            Utils.safeClose(os);
        }
    }

}
