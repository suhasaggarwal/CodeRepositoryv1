This javascript fetches Persona from RealTimePersonaFetch server if persona is not cached in user cookie.
Full Segment Sync Server Setup with tomcat threads configuration for wittyfeed scale is present here - 192.168.106.17.
It runs SegmentSync Application in tomcat which is fully configured and present in server.

Server.xml configuration in tomcat 7.0.64 -   
<Connector port="8443" maxHttpHeaderSize="8192" maxThreads="150" minSpareThreads="25" maxSpareThreads="75" enableLookups="false" disableUploadTimeout="true" acceptCount="100" scheme="https" secure="true" SSLEnabled="true" clientAuth="false" sslProtocol="TLS" keyAlias="mydomain" keystoreFile="/root/certtomcat/KeyStore.jks" keystorePass="qwerty" />
