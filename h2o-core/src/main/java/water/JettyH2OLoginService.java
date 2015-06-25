package water;

import org.eclipse.jetty.security.MappedLoginService;
import org.eclipse.jetty.security.PropertyUserStore;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.Scanner;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.security.Credential;

import java.io.IOException;

import org.eclipse.jetty.security.PropertyUserStore.UserListener;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.Scanner;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.security.Credential;

/* ------------------------------------------------------------ */
/**
 * Properties User Realm.
 *
 * An implementation of UserRealm that stores users and roles in-memory in HashMaps.
 * <P>
 * Typically these maps are populated by calling the load() method or passing a properties resource to the constructor. The format of the properties file is:
 *
 * <PRE>
 *  username: password [,rolename ...]
 * </PRE>
 *
 * Passwords may be clear text, obfuscated or checksummed. The class com.eclipse.Util.Password should be used to generate obfuscated passwords or password
 * checksums.
 *
 * If DIGEST Authentication is used, the password must be in a recoverable format, either plain text or OBF:.
 */
public class JettyH2OLoginService extends MappedLoginService implements PropertyUserStore.UserListener
{
  private static final Logger LOG = Log.getLogger(JettyH2OLoginService.class);

  private PropertyUserStore _propertyUserStore;
  private String _config;
  private Resource _configResource;
  private Scanner _scanner;
  private int _refreshInterval = 0;// default is not to reload

  /* ------------------------------------------------------------ */
  public JettyH2OLoginService()
  {
  }

  /* ------------------------------------------------------------ */
  public JettyH2OLoginService(String name)
  {
    setName(name);
  }

  /* ------------------------------------------------------------ */
  public JettyH2OLoginService(String name, String config)
  {
    setName(name);
    setConfig(config);
  }

  /* ------------------------------------------------------------ */
  public String getConfig()
  {
    return _config;
  }

  /* ------------------------------------------------------------ */
  public void getConfig(String config)
  {
    _config = config;
  }

  /* ------------------------------------------------------------ */
  public Resource getConfigResource()
  {
    return _configResource;
  }

    /* ------------------------------------------------------------ */
  /**
   * Load realm users from properties file. The property file maps usernames to password specs followed by an optional comma separated list of role names.
   *
   * @param config
   *            Filename or url of user properties file.
   */
  public void setConfig(String config)
  {
    _config = config;
  }

  /* ------------------------------------------------------------ */
  public void setRefreshInterval(int msec)
  {
    _refreshInterval = msec;
  }

  /* ------------------------------------------------------------ */
  public int getRefreshInterval()
  {
    return _refreshInterval;
  }

  /* ------------------------------------------------------------ */
  @Override
  protected UserIdentity loadUser(String username)
  {
    return null;
  }

  /* ------------------------------------------------------------ */
  @Override
  public void loadUsers() throws IOException
  {
    // TODO: Consider refactoring MappedLoginService to not have to override with unused methods
  }

    /* ------------------------------------------------------------ */
  /**
   * @see org.eclipse.jetty.util.component.AbstractLifeCycle#doStart()
   */
  @Override
  protected void doStart() throws Exception
  {
    super.doStart();

    if (_propertyUserStore == null)
    {
      if(LOG.isDebugEnabled())
        LOG.debug("doStart: Starting new PropertyUserStore. PropertiesFile: " + _config + " refreshInterval: " + _refreshInterval);

      _propertyUserStore = new PropertyUserStore();
      _propertyUserStore.setRefreshInterval(_refreshInterval);
      _propertyUserStore.setConfig(_config);
      _propertyUserStore.registerUserListener(this);
      _propertyUserStore.start();
    }
  }

    /* ------------------------------------------------------------ */
  /**
   * @see org.eclipse.jetty.util.component.AbstractLifeCycle#doStop()
   */
  @Override
  protected void doStop() throws Exception
  {
    super.doStop();
    if (_scanner != null)
      _scanner.stop();
    _scanner = null;
  }

  /* ------------------------------------------------------------ */
  @Override
  public void update(String userName, Credential credential, String[] roleArray)
  {
    if (LOG.isDebugEnabled())
      LOG.debug("update: " + userName + " Roles: " + roleArray.length);
    putUser(userName,credential,roleArray);
  }

  /* ------------------------------------------------------------ */
  @Override
  public void remove(String userName)
  {
    if (LOG.isDebugEnabled())
      LOG.debug("remove: " + userName);
    removeUser(userName);
  }

  /* ------------------------------------------------------------ */
  // TOM
  @Override
  public UserIdentity login(String username, Object credentials)
  {
    if (username == null)
      return null;

    UserIdentity user = _users.get(username);

    if (user==null)
      user = loadUser(username);

    if (user!=null)
    {
      UserPrincipal principal = (UserPrincipal)user.getUserPrincipal();
      if (principal.authenticate(credentials))
        return user;
    }
    return null;
  }

  /* ------------------------------------------------------------ */
  // TOM
  @Override
  public boolean validate(UserIdentity user)
  {
    if (_users.containsKey(user.getUserPrincipal().getName()))
      return true;

    if (loadUser(user.getUserPrincipal().getName())!=null)
      return true;

    return false;
  }
}

