package water;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.AbstractHandler;

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
    startHttps(port, baseport);
  }

  private void startHttp(int port, int baseport) throws Exception {
    if (port != 0) {
      int possiblePort = port + 1000;
      _server = new Server(possiblePort);
      registerHandlers();
      _server.start();
      _port = possiblePort;
    }
    else {
      while ((baseport + 1000) < (1<<16)) {
        try {
          int possiblePort = baseport + 1000;
          _server = new Server(possiblePort);
          registerHandlers();
          _server.start();
          _port = possiblePort;
          return;
        } catch (java.net.BindException e) {
          baseport += 2;
        }
      }

      throw new Exception("No available port found");
    }
  }

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
    registerHandlers();
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
  private void registerHandlers() {
    _server.setHandler(new H2OHandler());
  }

  /**
   * A handler class that passes basic Jetty requests Nano-style to H2O.
   */
  private static class H2OHandler extends AbstractHandler {
    public H2OHandler() {}

    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response) throws IOException, ServletException {
      // Marshal Jetty request parameters to Nano-style.
      // Nano serve() 'uri' parameter is Jetty handler 'target' parameter.
      String method = request.getMethod();

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
      NanoHTTPD.Response resp = water.api.RequestServer.SERVER.serve(target, method, headers, parms);

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

      // Mark Jetty request handled and return the response.
      baseRequest.setHandled(true);
    }
  }
}
