package gratum.source

import com.jcraft.jsch.ChannelSftp
import com.jcraft.jsch.JSch
import com.jcraft.jsch.Session
import com.jcraft.jsch.SftpATTRS
import gratum.etl.Pipeline

/**
 * SshSource allows you to connect to a remote SSH server and download one or more paths.  Paths can be either a directory
 * or a file.  You can specify as many directories or paths as you want to download and SshSource will send those InputStreams
 * through the Pipeline.  The SshSource rows will have the following keys:
 * <ul>
 *     <li>host - remote host of the SSH server</li>
 *     <li>port - remote port of the SSH server</li>
 *     <li>entry - remote ChannelSftp.LsEntry representing the current file</li>
 *     <li>filename - the remote filename of the current row</li>
 *     <li>stream - The java.io.InputStream that holds the contents of the remote file</li>
 * </ul>
 *
 * Example usage is the following:
 *
 * <code>
 * ssh( host )
 *   .knownHosts( knownHostFile )
 *   .authPass( username, password )
 *   .download( "/some/path/file.txt", "/some/other/path/file2.txt", "/other/directory")
 *   .into()
 *   ...
 *   .go()
 * </code>
 *
 * Using public key authentication:
 *
 * <code>
 * ssh( host, 2222 )
 *   .knownHosts( knownHostFile )
 *   .identity( username, keyFile, passphrase )
 *   .download( "/some/path/file.txt", "/some/other/path/file2.txt", "/other/directory")
 *   .into()
 *   ...
 *   .go()
 * </code>
 *
 * The known hosts file can be created using the following:
 *
 * <code>
 *     ssh-keyscan -H -t rsa example.org >> known_hosts
 * </code>
 */
class SshSource extends AbstractSource {

    JSch jsch
    String host
    int port = 22
    String username
    String password
    List<String> paths
    boolean strictHostCheck = true

    int line = 1

    SshSource(String host, int port) {
        super( host + ":" + port )
        jsch = new JSch()
        this.host = host
        this.port = port
    }

    /**
     * Returns a SshSource for file transfer over sftp connecting the given remote host
     * over the given TCP port.
     * @param remoteHost The remote host to connect to
     * @param port the remote TCP port to use to connect
     * @return this instance
     */
    public static SshSource ssh(String remoteHost, int port = 22) {
        return new SshSource( remoteHost, port )
    }

    /**
     * Authenticate with a username and password.
     * @param username The user name to authenticate
     * @param password The password for the given user name
     * @return this instance
     */
    public SshSource authPass(String username, String password) {
        this.username = username
        this.password = password
        return this
    }

    /**
     * This uses a SSH key pair to authenticate the given username.  This uses
     * public-key authentication of SSH.  The given passphrase is used to decrypt
     * the private key in the identity file.
     * @param username the username to authenticat as
     * @param identity the key file that holds the private key of the identity you wish to use
     * @param passphrase the passphrase that is used to protect the given identity file
     * @return this instance
     */
    public SshSource identity(String username, File identity, String passphrase) {
        this.username = username
        this.jsch.addIdentity( identity.getAbsolutePath(), passphrase )
        return this
    }

    /**
     * A file containing the list of known hosts the client will use to authorize
     * the connection to the server.
     * @param knownHostsFile The file of known hosts the client is willing to connect to.
     * @return this instance
     */
    public SshSource knownHosts(File knownHostsFile ) {
        jsch.setKnownHosts( knownHostsFile.absolutePath )
        return this
    }

    /**
     * Can be used to disable the known host checking, but this has serious security
     * implications which need to be carefully considered before doing this.  If you're
     * having problems with not being able to connect an safer option is to run the following:
     * <code>
     *     ssh-keyscan -H -t rsa example.org >> known_hosts
     * </code>
     *
     * Then use the file you created as the known host file.
     * @param enable if true enable strict host checking else disable strict host checking
     * @return this instance
     */
    public SshSource enableStrictHostCheck(boolean enable) {
        this.strictHostCheck = enable
        return this
    }

    /**
     * One or more remote paths you wish to download from the SFTP server.  These can
     * be individual files or directories.  If a directory is specified all files within the
     * directory are downloaded (recursively).
     * @param remotePath one or more remote paths you wish to download, file or directory is acceptable
     * @return this instance
     */
    public SshSource download(String... remotePath) {
        this.paths = remotePath.toList()
        return this
    }

    @Override
    void start(Pipeline pipeline) {
        Session jschSession = jsch.getSession(username, host, port)
        if( password ) jschSession.setPassword( password )
        jschSession.setConfig("PreferredAuthentications", "publickey,password")
        jschSession.setConfig("StrictHostKeyChecking", strictHostCheck ? "yes" : "no")
        jschSession.connect()
        try {
            ChannelSftp channel = (ChannelSftp)jschSession.openChannel("sftp")
            channel.connect()
            try {
                paths.each { String path ->
                    downloadPath( pipeline, channel, path )
                }
            } finally {
                channel.disconnect()
            }
        } finally {
            jschSession.disconnect()
        }
    }

    void downloadPath(Pipeline pipeline, ChannelSftp channel, String path) {
        SftpATTRS attributes = channel.stat(path)
        if( attributes.isDir() ) {
            Vector<ChannelSftp.LsEntry> directory = channel.ls( path )
            directory.each { ChannelSftp.LsEntry entry ->
                downloadPath( pipeline, channel, path + "/" + entry.filename)
            }
        } else {
            Vector<ChannelSftp.LsEntry> entry = channel.ls( path )
            String filename = ((ChannelSftp.LsEntry)entry.first()).filename
            pipeline.process( [host: host, port: port, entry: entry, filename: filename, stream: channel.get(path) ], line++)
        }
    }
}
