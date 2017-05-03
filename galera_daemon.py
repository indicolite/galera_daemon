#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Galera daemon for check
This is a simple daemon checking galera node state.
Usage  ./galera_daemon.py {start|stop|restart}
"""
import os
import sys
import re
import time
#import SocketServer
import logging
import logging.handlers
import atexit
from signal import SIGTERM
import MySQLdb
import yaml
import psutil
import subprocess, signal
import ConfigParser
reload(sys)
sys.path.insert(0, os.path.dirname(__file__))

class Daemon(object):
    """
    A generic daemon class.
    Usage: subclass the Daemon class and override the run() method
    """

    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null',
                 stderr='/dev/null'):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile

    def daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write(
                "fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write(
                "fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # write pidfile
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile, 'w+').write("%s\n" % pid)

    def delpid(self):
        """
        removes pidfile
        """
        os.remove(self.pidfile)

    def start(self):
        """
        Start the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            message = "pidfile %s already exist. Daemon already running?\n"
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)

        # Start the daemon
        self.daemonize()
        self.run()

    def stop(self):
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return  # not an error in a restart

        # Try killing the daemon process
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)

    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start()

    def run(self):
        """
        You should override this method when you subclass Daemon. It will be called after the process has been
        daemonized by start() or restart().
        """


class Config(object):
    """
    Create daemon configuration
    """
    def __init__(self, c_path='/etc/my.cnf.d/galera.yaml'):
        try:
            with open(c_path) as c_file:
                config_arr = yaml.safe_load(c_file)
        except IOError:
            config_arr = self.__defaults()

        self.__config = config_arr

    def get(self):
        """
        Returns config object
        """
        return self.__config

    @staticmethod
    def __defaults():
        """
        Default values of config object
        """
        arr = {
            'mysql':
                {
                    'host': '127.0.0.1',
                    'port': '3306',
                    'user': 'root',
                    'pass': 'password'
                },
            'daemon':
                {
                    'pid': '/tmp/galera-daemon.pid',
                    'critical_log': '/var/log/galera-daemon-critical.log',
                    'def_tty': '/dev/tty0',
                    'host': '0.0.0.0',
                    'port': '9876'
                },
            'logger':
                {
                    'location': '/var/log/galera-daemon.log',
                    'name': 'galera-log',
                    'rotation_time': '30'
                }
        }
        return arr


class LoggerMethod(object):
    """
    Logger class
    """
    def __init__(self, log_config):
        self.__config = log_config

    def setup_log(self):
        """
        Setups logger object for daemon logging
        """
        __log_location = self.__config['location']
        __logger = logging.getLogger(self.__config['name'])

        if len(__logger.handlers):
            return __logger
        else:
            logging.basicConfig(level=logging.DEBUG, filename=__log_location)
            __logger = logging.getLogger(self.__config['name'])
            formatter = logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s', datefmt='%d-%m-%Y %H:%M:%S %z')

            file_handler = logging.handlers.TimedRotatingFileHandler(filename=__log_location, when='midnight', backupCount=int(self.__config['rotation_time']))
            file_handler.setFormatter(formatter)
            file_handler.setLevel(logging.DEBUG)

            __logger.addHandler(file_handler)

            return __logger


class ServerRun(object):
    """
    Basic daemon server setup
    """
    def __init__(self, s_config):
        __logger_config = s_config['logger']
        __mysql_config = s_config['mysql']
        __daemon_config = s_config['daemon']

        self.__config = s_config
        self.__logger = LoggerMethod(__logger_config).setup_log()

        self.__socket_host = __daemon_config['host']
        self.__socket_port = __daemon_config['port']

        self.__db_user = __mysql_config['user']
        self.__db_pass = __mysql_config['pass']
        self.__db_host = __mysql_config['host']
        self.__db_port = int(__mysql_config['port'])
        self.__connection = None
        self.__logger.info("bingo mysql_config    %s", __mysql_config)

    def start_server(self):
        """
        Starts server on defined host and port with loop
        """
        try:
            while True:
                self.stat_check()
                ws_stat = self.parse_wsrep()
                if ws_stat.lower() != 'on':
                    self.__logger.info("local node wsrep_on config is not ON, please check once more")
                    time.sleep(60)
                    continue
                if self.ready_check() == 1 and self.sync_check() == 1 and self.clusterconn_check() == 1:
                    self.__logger.info("ready_check status is %s", self.ready_check())
                node_count = self.available_cluster()
                self.__logger.info("node_count is %d", node_count)
                if (self.ready_check() != 1) and node_count > 0:
                    result = self.mysqld_start()
                    self.__logger.info("mysqld_start status is %s", result)
                time.sleep(60)
        except BaseException as exception:
            self.__logger.error('Server wont start on %s:%s with error: %s', self.__socket_host, self.__socket_port, exception)
            raise


    def stat_check(self):
        """
        Checks if STAT of mysqld is T, thus change back to S.
        You may refer to man 7 signal to get more information
        """
        name = None
        pid = None
        stat = []
        p = subprocess.Popen(['ps', '-aux'], stdout=subprocess.PIPE)
        out, err = p.communicate()
        for line in out.splitlines():
            if 'mysqld' in line:
                name = line.split()[0]
                pid = int(line.split()[1])
                stat = line.split()[7]
        p = psutil.Process(pid)
        if "T" in stat:
            p.resume()
        return True

    def available_cluster(self):
        """
        Check available clusters to determine, if start or sleep
        """
        available_nodes = []
        local_cluster, clusters = self.parse_config()
        if not len(clusters):
            return False

        available_count = 0
        for _, node in enumerate(clusters):
            node = node.replace("\"", "")
            if node.strip() != '':
                available_nodes.append(node)
            #available_nodes.append(node)
            #if len(node) == 0 or node.strip() == '':
            #    available_nodes.remove(node)
            #    break

        for node in available_nodes:
            try:
                database_conn = MySQLdb.connect(
                        host=node,
                        user=self.__db_user,
                        passwd=self.__db_pass,
                        port=self.__db_port)
                self.__connection = database_conn
                self.__logger.info("Available node %s with connect %s", node, database_conn)
                cursor = self.__connection.cursor()
                cursor.execute("show status like 'wsrep_connected';")
                result = cursor.fetchone()
                if result[1] == 'ON':
                    available_count += 1
                    self.__logger.info("available counts now + 1 = %d", available_count)
                else:
                    self.__logger.info("available counts still with %d", available_count)
            except BaseException:
                self.__logger.info("Now node %s connect failed", node)

            #finally:
            #    #cursor = self.__connection.cursor()
            #    #cursor.execute("show status like 'wsrep_connected';")
            #    #result = cursor.fetchone()
            #    self.__connection.close()
            #    break

            #if result[1] == 'ON':
            #    available_count += 1
            #    self.__logger.info("available counts now + 1 = %d", available_count)
            #else:
            #    self.__logger.info("available counts still with %d", available_count)

        self.__logger.info("available nodes finally %s, available_count %d", available_nodes, available_count)
        return available_count


    def mysqld_start(self):
        """
        Checks if mysqld process exists,
        If not, start a new mysqld process
        """
        p = subprocess.Popen(['ps', '-aux'], stdout=subprocess.PIPE)
        out, err = p.communicate()
        for line in out.splitlines():
            if 'mysqld' in line:
                return True
        proc = subprocess.call('systemctl restart mariadb', shell=True)
        if proc != 0:
            return False
        return True

    def ready_check(self):
        """
        Check if node is ready for operations
        """
        try:
            database_conn = MySQLdb.connect(
                host=self.__db_host,
                user=self.__db_user,
                passwd=self.__db_pass,
                port=self.__db_port)
            self.__connection = database_conn
            self.__logger.info("ready database connect %s", database_conn)
        except BaseException:
            return 'ready connect failed'

        cursor = self.__connection.cursor()
        cursor.execute("show status like 'wsrep_ready';")
        result = cursor.fetchone()
        self.__connection.close()

        if result[1] == 'ON':
            return True
        else:
            answer = 'status_err'+self.__ready_check.__name__
            return False


    def sync_check(self):
        """
        Checks if UUID of cluster and node is the same
        """
        try:
            database_conn = MySQLdb.connect(
                host=self.__db_host,
                user=self.__db_user,
                passwd=self.__db_pass,
                port=self.__db_port)
            self.__connection = database_conn
            self.__logger.info("sync database connect %s", database_conn)
        except BaseException:
            return 'sync connect failed'

        cursor = self.__connection.cursor()
        cursor.execute("show status like 'wsrep_cluster_state_uuid'")
        cluster_result = cursor.fetchone()
        cursor.execute("show status like 'wsrep_local_state_uuid'")
        local_result = cursor.fetchone()
        self.__connection.close()

        if cluster_result[1] == local_result[1]:
            answer = 'ok'
            self.__logger.info(" %s %s sync check", cluster_result[1], local_result[1])
            return True
        else:
            answer = 'status_err'+self.__sync_check.__name__
            return False


    def clusterconn_check(self):
        """
        Check if node is connected to cluster
        """
        try:
            database_conn = MySQLdb.connect(
                host=self.__db_host,
                user=self.__db_user,
                passwd=self.__db_pass,
                port=self.__db_port)
            self.__connection = database_conn
            self.__logger.info("cluster database connect %s", database_conn)
        except BaseException:
            return 'cluster connect failed'

        cursor = self.__connection.cursor()
        cursor.execute("show status like 'wsrep_connected';")
        result = cursor.fetchone()
        self.__connection.close()

        if result[1] == 'ON':
            self.__logger.info("clusterconn check %s", result)
            return True
        else:
            answer = 'status_err'+self.__provider_connected_check.__name__
            return False

    def parse_config(self):
        """
        Parse the galera.cnf Configuration file
        """
        config_file_path = "/etc/my.cnf.d/galera.cnf"
        config = ConfigParser.RawConfigParser()
        config.read(config_file_path)
        try:
            cluster_address = config.get("mysqld", "wsrep_cluster_address")
        except ConfigParser.NoOptionError:
            self.__logger.info("Couldn't find wsrep_cluster_address setting in %s", config_file_path)
            sys.exit(1)
        nodes = cluster_address.replace("gcomm://", "").split(",")
        try:
            local_node = config.get("mysqld", "wsrep_node_address")
        except ConfigParser.NoOptionError:
            self.__logger.info("Couldn't find wsrep_node_address setting in %s", config_file_path)
            sys.exit(1)
        return (local_node, nodes)

    def parse_wsrep(self):
        """
        Parse the galera.cnf Configuration file to get wsrep_on
        """
        config_file_path = "/etc/my.cnf.d/galera.cnf"
        config = ConfigParser.RawConfigParser()
        config.read(config_file_path)
        try:
            wsrep_stat = config.get("galera", "wsrep_on")
        except ConfigParser.NoOptionError:
            self.__logger.info("Couldn't find wsrep_on setting in %s", config_file_path)
            sys.exit(1)
        return wsrep_stat


class GaleraDaemon(Daemon):
    """
    Daemon init class
    """
    def __init__(self, pidfile, s_config=None):
        self.__config = s_config
        self.stdin = '/dev/null'
        self.stdout = self.__config['daemon']['def_tty']
        self.stderr = self.__config['daemon']['critical_log']
        self.pidfile = pidfile
        super(GaleraDaemon, self).__init__(self.pidfile)

    def run(self):
        application = ServerRun(self.__config)
        application.start_server()

    #@staticmethod
    #def connections_on_port(port):
    #    """
    #    Counts open connections on given port
    #    """
    #    cnt = 0

    #    for connection in psutil.net_connections():
    #        laddr = connection[3]

    #        if laddr[1] == int(port):
    #            cnt += 1

    #    return cnt


class StartDaemon(object):
    """
    Class provides logic to execute start / stop statements of daemon
    """
    def __init__(self):
        self.__config = Config().get()
        self.__logger = LoggerMethod(self.__config['logger']).setup_log()
        self.__daemon = GaleraDaemon(self.__config['daemon']['pid'],
                                     s_config=self.__config)

    def execute(self, command):
        """
        Method executes start / stop / restart statements
        """
        if command not in ['start', 'stop', 'restart']:
            print "usage: {} start|stop|restart".format(command)
            self.__logger.info('Bad %s usage', command)
            sys.exit(2)

        if command == 'start':
            self.__logger.info('Application has been started')
            self.__daemon.start()

        elif command == 'stop':
            print >> sys.stdout, 'Application has been stopped.'
            self.__logger.info('Application has been stopped')
            self.__daemon.stop()
        elif command == 'restart':
            self.__logger.warning('Application has been restarted')
            self.__daemon.restart()
        sys.exit(0)

if __name__ == "__main__":
    if len(sys.argv) == 2:
        START_IT = StartDaemon()
        START_IT.execute(sys.argv[1])
    else:
        print "Unknown command. Usage: {} start|stop|restart".format(sys.argv[0])
        sys.exit(2)
