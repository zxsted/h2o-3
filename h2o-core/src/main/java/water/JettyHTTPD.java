package water;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Collections;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.security.*;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import water.util.FileUtils;

/**
 * Embedded Jetty instance inside H2O.
 * This is intended to be a singleton per H2O node.
 */
public class JettyHTTPD {
  // The actual port chosen port number.
  private int _port;

  // Jetty server object.
  private Server _server;

  /**
   * Create bare Jetty object.
   */
  public JettyHTTPD() {
  }

  /**
   * Returns the actual chosen port number by the start() method.
   * @return Port number
   */
  int getPort() { return _port; }

  /**
   * Choose a port and start the Jetty server.
   *
   * @param port If specified, this exact port must be used successfully or an exception is thrown.
   * @param baseport Base port number to start looking for ports at.  Walk up to find a free one.
   *                 If none is found before crossing 65k, throw an exception.
   * @throws Exception
   */
  public void start(int port, int baseport) throws Exception {
    startHttp(port, baseport);
  }

  private void createServer(int port) throws Exception {
    _server = new Server(port);

    boolean hasCustomAuthorizationHandler = true;
    if (hasCustomAuthorizationHandler) {
      // REFER TO http://www.eclipse.org/jetty/documentation/9.1.4.v20140401/embedded-examples.html#embedded-secured-hello-handler

      // CustomAuthorizationService.createHandler(_server, new H2OHandler());

      // Dummy login service - replace with pluggable auth.
      LoginService loginService = new HashLoginService("H2O","realm.properties");
//      LoginService loginService = new JettyH2OLoginService("H2O", "realm.properties");
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
    _port = port;
  }

  private void startHttp(int port, int baseport) throws Exception {
    if (port != 0) {
      int possiblePort = port + 1000;
      createServer(possiblePort);
    }
    else {
      while ((baseport + 1000) < (1<<16)) {
        try {
          int possiblePort = baseport + 1000;
          createServer(possiblePort);
          return;
        } catch (java.net.BindException e) {
          baseport += 2;
        }
      }

      throw new Exception("No available port found");
    }
  }

  /**
   * This implementation is based on http://blog.denevell.org/jetty-9-ssl-https.html
   *
   * @param port See start()
   * @param baseport See start()
   * @throws Exception
   */
  private void startHttps(int port, int baseport) throws Exception {
    int httpsPort = ((port != 0) ? port : baseport) + 1000;

    _server = new Server();
    HttpConfiguration http_config = new HttpConfiguration();
    http_config.setSecureScheme("https");
    http_config.setSecurePort(httpsPort);

    HttpConfiguration https_config = new HttpConfiguration(http_config);
    https_config.addCustomizer(new SecureRequestCustomizer());

    SslContextFactory sslContextFactory = new SslContextFactory("h2o.jks");
    sslContextFactory.setKeyStorePassword("h2oh2o");

    ServerConnector httpsConnector =
            new ServerConnector(_server,
                                new SslConnectionFactory(sslContextFactory, "http/1.1"),
                                new HttpConnectionFactory(https_config));
    httpsConnector.setPort(httpsPort);

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
    _server.stop();
  }

  /**
   * Hook up Jetty handlers.  Do this before start() is called.
   */
  private void registerHandlers(HandlerWrapper s) {
    ServletContextHandler context = new ServletContextHandler(
            ServletContextHandler.SECURITY | ServletContextHandler.SESSIONS
    );
    context.setContextPath("/");
    s.setHandler(context);

    context.addServlet(H2oNpsBinServlet.class, "/3/NodePersistentStorage.bin/*");
    context.addServlet(H2oDefaultServlet.class, "/");
  }

  @SuppressWarnings("serial")
  public static class H2oNpsBinServlet extends HttpServlet
  {
    @Override
    protected void doGet( HttpServletRequest request,
                          HttpServletResponse response ) throws IOException, ServletException {
      setCommonResponseHttpHeaders(response);
      response.setContentType("application/octet-stream");
      response.setStatus(HttpServletResponse.SC_OK);
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
}
