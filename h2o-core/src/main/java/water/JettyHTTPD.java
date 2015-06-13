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

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import org.eclipse.jetty.server.Server;
import water.util.FileUtils;

public class JettyHTTPD {
  private int _port;
  private Server _server;

  public JettyHTTPD() {
  }

  int getPort() { return _port; }

  public void start(int port, int baseport) throws Exception {
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
          break;
        } catch (java.net.BindException e) {
          baseport += 2;
        }
      }
    }
  }

  public void stop() throws Exception {
    _server.stop();
  }

  private void registerHandlers() {
    _server.setHandler(new H2OHandler());
  }

  public static class H2OHandler extends AbstractHandler {
    public H2OHandler() {}

    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response) throws IOException, ServletException {
      // Marshal Jetty request parameters to Nano-style.
      // Nano serve() uri parameter is target.
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
