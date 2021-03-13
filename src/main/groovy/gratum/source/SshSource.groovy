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
 */
class SshSource extends AbstractSource {

    JSch jsch
    String host
    int port = 22
    String username
    String password
    List<String> paths

    SshSource(String host, int port) {
        this.name = host + ":" + port
        jsch = new JSch()
        this.host = host
        this.port = port
    }

    public static SshSource ssh(String remoteHost, int port = 22) {
        return new SshSource( remoteHost, port )
    }

    public SshSource authPass(String username, String password) {
        this.username = username
        this.password = password
        return this
    }

    public SshSource identity(String username, File identity, String passphrase) {
        this.username = username
        this.jsch.addIdentity( identity.getAbsolutePath(), passphrase )
        return this
    }

    public SshSource knownHosts(File knownHostsFile ) {
        jsch.setKnownHosts( knownHostsFile.absolutePath )
        return this
    }

    public SshSource download(String... remotePath) {
        this.paths = remotePath.toList()
        return this
    }

    @Override
    void start(Pipeline pipeline) {
        Session jschSession = jsch.getSession(username, host, port)
        if( password ) jschSession.setPassword( password )
        jschSession.setConfig("PreferredAuthentications", "publickey,password")
        jschSession.connect()
        try {
            ChannelSftp channel = (ChannelSftp)jschSession.openChannel("sftp")
            channel.connect()
            try {
                int line = 1
                paths.each { String path ->
                    SftpATTRS attributes = channel.stat(path)
                    if( attributes.isDir() ) {
                        Vector<ChannelSftp.LsEntry> directory = channel.ls( path )
                        directory.each { ChannelSftp.LsEntry entry ->
                            pipeline.process( [ host: host, port: port, entry: entry, filename: entry.filename, stream: channel.get(path + "/" + entry.filename) ], line++ )
                        }
                    } else {
                        Vector<ChannelSftp.LsEntry> entry = channel.ls( path )
                        String filename = ((ChannelSftp.LsEntry)entry.first()).filename
                        pipeline.process( [host: host, port: port, filename: filename, stream: channel.get(path) ], line++)
                    }
                }
            } finally {
                channel.disconnect()
            }
        } finally {
            jschSession.disconnect()
        }
    }
}
