package water;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URLDecoder;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.jaas.JAASLoginService;
import org.eclipse.jetty.security.*;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import water.api.H2OErrorV3;
import water.exceptions.H2OAbstractRuntimeException;
import water.exceptions.H2OFailException;
import water.fvec.UploadFileVec;
import water.init.NodePersistentStorage;
import water.util.FileUtils;
import water.util.HttpResponseStatus;
import water.util.Log;

/**
 * Embedded Jetty instance inside H2O.
 * This is intended to be a singleton per H2O node.
 */
public class JettyHTTPD {
  // The actual port chosen port number.
  private int _port;
  private String _ip;
  private static volatile boolean _acceptRequests = false;

  // Jetty server object.
  private Server _server;

  /**
   * Create bare Jetty object.
   */
  public JettyHTTPD(int port, String ip) {
    _port = port;
    _ip = ip;
    System.setProperty("org.eclipse.jetty.server.Request.maxFormContentSize", Integer.toString(Integer.MAX_VALUE));
  }

  /**
   * Returns the actual chosen port number by the start() method.
   * @return Port number
   */
  int getPort() { return _port; }

  /**
   * Choose a port and start the Jetty server.
   *
   * @throws Exception
   */
  public void start() throws Exception {
    startHttp();
  }

  public void acceptRequests() {
    _acceptRequests = true;
  }

  private void createServer() throws Exception {
    _server = new Server();
    ServerConnector connector=new ServerConnector(_server);
    if (_ip != null) {
      connector.setHost(_ip);
    }
    connector.setPort(_port);
    _server.setConnectors(new Connector[]{connector});

    boolean hasCustomAuthorizationHandler = false;
    if (hasCustomAuthorizationHandler) {
      // REFER TO http://www.eclipse.org/jetty/documentation/9.1.4.v20140401/embedded-examples.html#embedded-secured-hello-handler

      // CustomAuthorizationService.createHandler(_server, new H2OHandler());

      // Dummy login service - replace with pluggable auth.
      LoginService loginService = new HashLoginService("H2O","realm.properties");
      JAASLoginService ls = new JAASLoginService();
      IdentityService identityService = new DefaultIdentityService();
      loginService.setIdentityService(identityService);
      _server.addBean(loginService);

      // Set a security handler as the first handler in the chain.
      ConstraintSecurityHandler security = new ConstraintSecurityHandler();
      _server.setHandler(security);

      // Set up a constraint to authenticate all calls, and allow certain roles in.
      Constraint constraint = new Constraint();
      constraint.setName("auth");
      constraint.setAuthenticate( true );
      constraint.setRoles(new String[]{"user", "admin"});

      ConstraintMapping mapping = new ConstraintMapping();
      mapping.setPathSpec( "/*" ); // Lock down all API calls
      mapping.setConstraint(constraint);
      security.setConstraintMappings(Collections.singletonList(mapping));

      // Authentication / Authorization
      security.setAuthenticator(new BasicAuthenticator());
      security.setLoginService(loginService);

      // Pass-through to H2O if authenticated.
      registerHandlers(security);
    } else {
      registerHandlers(_server);
    }

    _server.start();
  }

  private void startHttp() throws Exception {
    createServer();
  }

  /**
   * This implementation is based on http://blog.denevell.org/jetty-9-ssl-https.html
   *
   * @throws Exception
   */
  private void startHttps() throws Exception {
    _server = new Server();
    HttpConfiguration http_config = new HttpConfiguration();
    http_config.setSecureScheme("https");
    http_config.setSecurePort(_port);

    HttpConfiguration https_config = new HttpConfiguration(http_config);
    https_config.addCustomizer(new SecureRequestCustomizer());

    SslContextFactory sslContextFactory = new SslContextFactory("h2o.jks");
    sslContextFactory.setKeyStorePassword("h2oh2o");

    ServerConnector httpsConnector =
            new ServerConnector(_server,
                                new SslConnectionFactory(sslContextFactory, "http/1.1"),
                                new HttpConnectionFactory(https_config));
    if (_ip != null) {
      httpsConnector.setHost(_ip);
    }
    httpsConnector.setPort(_port);

    _server.setConnectors(new Connector[]{ httpsConnector });
    registerHandlers(_server);
    _server.start();
  }

  /**
   * Stop Jetty server after it has been started.
   * This is unlikely to ever be called by H2O until H2O supports graceful shutdown.
   *
   * @throws Exception
   */
  public void stop() throws Exception {
    if (_server != null) {
      _server.stop();
    }
  }

  /**
   * Hook up Jetty handlers.  Do this before start() is called.
   */
  private void registerHandlers(HandlerWrapper s) {
    GateHandler gh = new GateHandler();

    ServletContextHandler context = new ServletContextHandler(
            ServletContextHandler.SECURITY | ServletContextHandler.SESSIONS
    );
    context.setContextPath("/");

    context.addServlet(H2oNpsBinServlet.class,   "/3/NodePersistentStorage.bin/*");
    context.addServlet(H2oPostFileServlet.class, "/3/PostFile.bin");
    context.addServlet(H2oPostFileServlet.class, "/3/PostFile");
    context.addServlet(H2oDefaultServlet.class,  "/");

    Handler[] handlers = {gh, context};
    HandlerCollection hc = new HandlerCollection();
    hc.setHandlers(handlers);
    s.setHandler(hc);
  }

  public class GateHandler extends AbstractHandler {
    public GateHandler() {}

    public void handle( String target,
                        Request baseRequest,
                        HttpServletRequest request,
                        HttpServletResponse response ) throws IOException, ServletException {
      while (! _acceptRequests) {
        try {
          Thread.sleep(100);
        }
        catch (Exception ignore) {}
      }
    }
  }

  public static class H2oNpsBinServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response) throws IOException, ServletException {
      String uri = getDecodedUri(request);
      try {
        setCommonResponseHttpHeaders(response);

        Pattern p = Pattern.compile(".*/NodePersistentStorage.bin/([^/]+)/([^/]+)");
        Matcher m = p.matcher(uri);
        boolean b = m.matches();
        if (!b) {
          response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
          response.getWriter().write("Improperly formatted URI");
          return;
        }

        String categoryName = m.group(1);
        String keyName = m.group(2);
        NodePersistentStorage nps = H2O.getNPS();
        AtomicLong length = new AtomicLong();
        InputStream is = nps.get(categoryName, keyName, length);
        response.setContentType("application/octet-stream");
        response.setContentLengthLong(length.get());
        response.addHeader("Content-Disposition", "attachment; filename=" + keyName + ".flow");
        response.setStatus(HttpServletResponse.SC_OK);
        OutputStream os = response.getOutputStream();
        water.util.FileUtils.copyStream(is, os, 2048);
      } catch (Exception e) {
        sendErrorResponse(response, e, uri);
      }
    }

    @Override
    protected void doPost(HttpServletRequest request,
                          HttpServletResponse response) throws IOException, ServletException {
      String uri = getDecodedUri(request);
      try {
        setCommonResponseHttpHeaders(response);

        Pattern p = Pattern.compile(".*NodePersistentStorage.bin/([^/]+)/([^/]+)");
        Matcher m = p.matcher(uri);
        boolean b = m.matches();
        if (!b) {
          response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
          response.getWriter().write("Improperly formatted URI");
          return;
        }

        String categoryName = m.group(1);
        String keyName = m.group(2);

        InputStream is = extractPartInputStream(request, response);
        if (is == null) {
          return;
        }

        H2O.getNPS().put(categoryName, keyName, is);
        long length = H2O.getNPS().get_length(categoryName, keyName);
        String responsePayload = "{ " +
                "\"category\" : "     + "\"" + categoryName + "\", " +
                "\"name\" : "         + "\"" + keyName      + "\", " +
                "\"total_bytes\" : "  +        length       + " " +
                "}\n";
        response.setContentType("application/json");
        response.getWriter().write(responsePayload);
      } catch (Exception e) {
        sendErrorResponse(response, e, uri);
      }
    }
  }

  public static class H2oPostFileServlet extends HttpServlet {
    @Override
    protected void doPost(HttpServletRequest request,
                          HttpServletResponse response) throws IOException, ServletException {
      String uri = getDecodedUri(request);

      try {
        setCommonResponseHttpHeaders(response);

        String destination_frame = request.getParameter("destination_frame");
        if (destination_frame == null) {
          destination_frame = "upload" + Key.rand();
        }
        if (!validKeyName(destination_frame)) {
          response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
          response.getWriter().write("Invalid key name, contains illegal characters");
          return;
        }

        //
        // Here is an example of how to upload a file from the command line.
        //
        // curl -v -F "file=@allyears2k_headers.zip" "http://localhost:54321/PostFile.bin?destination_frame=a.zip"
        //
        // JSON Payload returned is:
        //     { "destination_frame": "key_name", "total_bytes": nnn }
        //
        InputStream is = extractPartInputStream(request, response);
        if (is == null) {
          return;
        }

        UploadFileVec.ReadPutStats stats = new UploadFileVec.ReadPutStats();
        UploadFileVec.readPut(destination_frame, is, stats);
        String responsePayload = "{ "       +
                "\"destination_frame\": \"" + destination_frame   + "\", " +
                "\"total_bytes\": "         + stats.total_bytes + " " +
                "}\n";
        response.setContentType("application/json");
        response.getWriter().write(responsePayload);
      }
      catch (Exception e) {
        sendErrorResponse(response, e, uri);
      }
    }
  }

  private static InputStream extractPartInputStream (HttpServletRequest request, HttpServletResponse response) throws IOException{
    String ct = request.getContentType();
    if (! ct.startsWith("multipart/form-data")) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      response.getWriter().write("Content type must be multipart/form-data");
      return null;
    }

    String boundaryString;
    int idx = ct.indexOf("boundary=");
    if (idx < 0) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      response.getWriter().write("Boundary missing");
      return null;
    }

    boundaryString = ct.substring(idx + "boundary=".length());
    byte[] boundary = boundaryString.getBytes();

    // Consume headers of the mime part.
    InputStream is = request.getInputStream();
    String line = readLine(is);
    while ((line != null) && (line.trim().length()>0)) {
      line = readLine(is);
    }

    InputStreamWrapper isw = new InputStreamWrapper(is, boundary);
    return isw;
  }

  private static boolean validKeyName(String name) {
    byte[] arr = name.getBytes();
    for (byte b : arr) {
      if (b == '"') return false;
      if (b == '\\') return false;
    }

    return true;
  }

  private static void sendErrorResponse(HttpServletResponse response, Exception e, String uri) {
    if (e instanceof H2OFailException) {
      H2OFailException ee = (H2OFailException) e;
      H2OError error = ee.toH2OError(uri);

      Log.fatal("Caught exception (fatal to the cluster): " + error.toString());
      H2O.fail(error.toString());
    }
    else if (e instanceof H2OAbstractRuntimeException) {
      H2OAbstractRuntimeException ee = (H2OAbstractRuntimeException) e;
      H2OError error = ee.toH2OError(uri);

      Log.warn("Caught exception: " + error.toString());
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);

      // Note: don't use Schema.schema(version, error) because we have to work at bootstrap:
      try {
        response.getWriter().write(new H2OErrorV3().fillFromImpl(error).toJsonString());
      }
      catch (Exception ignore) {}
    }
    else { // make sure that no Exception is ever thrown out from the request
      H2OError error = new H2OError(e, uri);

      // some special cases for which we return 400 because it's likely a problem with the client request:
      if (e instanceof IllegalArgumentException)
        error._http_status = HttpResponseStatus.BAD_REQUEST.getCode();
      else if (e instanceof FileNotFoundException)
        error._http_status = HttpResponseStatus.BAD_REQUEST.getCode();
      else if (e instanceof MalformedURLException)
        error._http_status = HttpResponseStatus.BAD_REQUEST.getCode();
      response.setStatus(error._http_status);

      Log.warn("Caught exception: " + error.toString());

      // Note: don't use Schema.schema(version, error) because we have to work at bootstrap:
      try {
        response.getWriter().write(new H2OErrorV3().fillFromImpl(error).toJsonString());
      }
      catch (Exception ignore) {}
    }
  }

  private static String getDecodedUri(HttpServletRequest request) {
    try {
      String s = URLDecoder.decode(request.getRequestURI(), "UTF-8");
      return s;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void setCommonResponseHttpHeaders(HttpServletResponse response) {
    response.setHeader("X-h2o-build-project-version", H2O.ABV.projectVersion());
    response.setHeader("X-h2o-rest-api-version-max", Integer.toString(water.api.RequestServer.H2O_REST_API_VERSION));
    response.setHeader("X-h2o-cluster-id", Long.toString(H2O.CLUSTER_ID));
  }

  @SuppressWarnings("serial")
  public static class H2oDefaultServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response) throws IOException, ServletException {
      doGeneric("GET", request, response);
    }

    @Override
    protected void doPost(HttpServletRequest request,
                         HttpServletResponse response) throws IOException, ServletException {
      doGeneric("POST", request, response);
    }

    @Override
    protected void doHead(HttpServletRequest request,
                          HttpServletResponse response) throws IOException, ServletException {
      doGeneric("HEAD", request, response);
    }

    @Override
    protected void doDelete(HttpServletRequest request,
                          HttpServletResponse response) throws IOException, ServletException {
      doGeneric("DELETE", request, response);
    }

    @Override
    protected void doPut(HttpServletRequest request,
                            HttpServletResponse response) throws IOException, ServletException {
      doGeneric("PUT", request, response);
    }

    public void doGeneric(String method,
                          HttpServletRequest request,
                          HttpServletResponse response) throws IOException, ServletException {
      // Marshal Jetty request parameters to Nano-style.

      // Note that getServletPath does an un-escape so that the %24 of job id's are turned into $ characters.
      String uri = request.getServletPath();

      Properties headers = new Properties();
      Enumeration<String> en = request.getHeaderNames();
      while (en.hasMoreElements()) {
        String key = en.nextElement();
        String value = request.getHeader(key);
        headers.put(key, value);
      }

      Properties parms = new Properties();
      Map<String, String[]> parameterMap;
      parameterMap = request.getParameterMap();
      for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
        String key = entry.getKey();
        String[] values = entry.getValue();
        for (String value : values) {
          parms.put(key, value);
        }
      }

      // Make Nano call.
      NanoHTTPD.Response resp = water.api.RequestServer.SERVER.serve(uri, method, headers, parms);

      // Un-marshal Nano response back to Jetty.
      String choppedNanoStatus = resp.status.substring(0, 3);
      assert(choppedNanoStatus.length() == 3);
      int sc = Integer.parseInt(choppedNanoStatus);
      response.setStatus(sc);

      response.setContentType(resp.mimeType);

      Properties header = resp.header;
      Enumeration<Object> en2 = header.keys();
      while (en2.hasMoreElements()) {
        String key = (String) en2.nextElement();
        String value = header.getProperty(key);
        response.setHeader(key, value);
      }

      OutputStream os = response.getOutputStream();
      InputStream is = resp.data;
      FileUtils.copyStream(is, os, 1024);
    }
  }

  //--------------------------------------------------

  private static String readLine(InputStream in) throws IOException {
    StringBuilder sb = new StringBuilder();
    byte[] mem = new byte[1024];
    while (true) {
      int sz = readBufOrLine(in,mem);
      sb.append(new String(mem,0,sz));
      if (sz < mem.length)
        break;
      if (mem[sz-1]=='\n')
        break;
    }
    if (sb.length()==0)
      return null;
    String line = sb.toString();
    if (line.endsWith("\r\n"))
      line = line.substring(0,line.length()-2);
    else if (line.endsWith("\n"))
      line = line.substring(0,line.length()-1);
    return line;
  }

  private static int readBufOrLine(InputStream in, byte[] mem) throws IOException {
    byte[] bb = new byte[1];
    int sz = 0;
    while (true) {
      byte b;
      byte b2;
      if (sz==mem.length)
        break;
      try {
        in.read(bb,0,1);
        b = bb[0];
        mem[sz++] = b;
      } catch (EOFException e) {
        break;
      }
      if (b == '\n')
        break;
      if (sz==mem.length)
        break;
      if (b == '\r') {
        try {
          in.read(bb,0,1);
          b2 = bb[0];
          mem[sz++] = b2;
        } catch (EOFException e) {
          break;
        }
        if (b2 == '\n')
          break;
      }
    }
    return sz;
  }

  private static final class InputStreamWrapper extends InputStream {
    static final byte[] BOUNDARY_PREFIX = { '\r', '\n', '-', '-' };
    final InputStream _wrapped;
    final byte[] _boundary;
    final byte[] _lookAheadBuf;
    int _lookAheadLen;

    public InputStreamWrapper(InputStream is, byte[] boundary) {
      _wrapped = is;
      _boundary = Arrays.copyOf(BOUNDARY_PREFIX, BOUNDARY_PREFIX.length + boundary.length);
      System.arraycopy(boundary, 0, _boundary, BOUNDARY_PREFIX.length, boundary.length);
      _lookAheadBuf = new byte[_boundary.length];
      _lookAheadLen = 0;
    }

    @Override public void close() throws IOException { _wrapped.close(); }
    @Override public int available() throws IOException { return _wrapped.available(); }
    @Override public long skip(long n) throws IOException { return _wrapped.skip(n); }
    @Override public void mark(int readlimit) { _wrapped.mark(readlimit); }
    @Override public void reset() throws IOException { _wrapped.reset(); }
    @Override public boolean markSupported() { return _wrapped.markSupported(); }

    @Override public int read() throws IOException { throw new UnsupportedOperationException(); }
    @Override public int read(byte[] b) throws IOException { return read(b, 0, b.length); }
    @Override public int read(byte[] b, int off, int len) throws IOException {
      if(_lookAheadLen == -1)
        return -1;
      int readLen = readInternal(b, off, len);
      if (readLen != -1) {
        int pos = findBoundary(b, off, readLen);
        if (pos != -1) {
          _lookAheadLen = -1;
          return pos - off;
        }
      }
      return readLen;
    }

    private int readInternal(byte b[], int off, int len) throws IOException {
      if (len < _lookAheadLen ) {
        System.arraycopy(_lookAheadBuf, 0, b, off, len);
        _lookAheadLen -= len;
        System.arraycopy(_lookAheadBuf, len, _lookAheadBuf, 0, _lookAheadLen);
        return len;
      }

      if (_lookAheadLen > 0) {
        System.arraycopy(_lookAheadBuf, 0, b, off, _lookAheadLen);
        off += _lookAheadLen;
        len -= _lookAheadLen;
        int r = Math.max(_wrapped.read(b, off, len), 0) + _lookAheadLen;
        _lookAheadLen = 0;
        return r;
      } else {
        return _wrapped.read(b, off, len);
      }
    }

    private int findBoundary(byte[] b, int off, int len) throws IOException {
      int bidx = -1; // start index of boundary
      int idx = 0; // actual index in boundary[]
      for(int i = off; i < off+len; i++) {
        if (_boundary[idx] != b[i]) { // reset
          idx = 0;
          bidx = -1;
        }
        if (_boundary[idx] == b[i]) {
          if (idx == 0) bidx = i;
          if (++idx == _boundary.length) return bidx; // boundary found
        }
      }
      if (bidx != -1) { // it seems that there is boundary but we did not match all boundary length
        assert _lookAheadLen == 0; // There should not be not read lookahead
        _lookAheadLen = _boundary.length - idx;
        int readLen = _wrapped.read(_lookAheadBuf, 0, _lookAheadLen);
        if (readLen < _boundary.length - idx) { // There is not enough data to match boundary
          _lookAheadLen = readLen;
          return -1;
        }
        for (int i = 0; i < _boundary.length - idx; i++)
          if (_boundary[i+idx] != _lookAheadBuf[i])
            return -1; // There is not boundary => preserve lookahead buffer
        // Boundary found => do not care about lookAheadBuffer since all remaining data are ignored
      }

      return bidx;
    }
  }
}
