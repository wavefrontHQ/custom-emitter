"""
Simple custom emitter for DataDog agent to submit metrics to Wavefront.

Enable it by adding the following lines to datadog.conf and restarting
the agent:

wf_host: wavefront-proxy-hostname
custom_emitters: /path/to/wavefrontEmitter.py

See the emitter documentation for additional optional settings.

Version: 0.9.2
"""

import socket
import sys

# pylint: disable=invalid-name
class emitter(object):
    """
    Custom emitter for DataDog to submit metrics to the Wavefront proxy.
    This emitter requires that the configuration have 2 additional items:
       wf_host  the name/ip of the Wavefront proxy host
                (required: emitter will do nothing if not set)
       wf_port  the port that the proxy is listening on in Wavefront format
                (optional: default is 2878)
       wf_dry_run (yes|true) means "dry run" (just print the data and don't
                actually send
                (optional: default is no)
       wf_meta_tags comma separated list of tags to extract as point tags
                from meta dictionary in collector JSON
                (optional: default is empty list)
    From the custom emitter documentation in datadog.conf:
    If the name of the emitter function is not specified, 'emitter' is assumed.
    We are naming the class "emitter" to keep things as simple as possible for
    configuration.
    """

    def __init__(self):
        self.proxy_dry_run = True
        self.sock = None
        self.point_tags = {}
        self.source_tags = []
        self.meta_tags = []

    # pylint: disable=too-many-branches
    def __call__(self, message, log, agent_config):
        """
        __call__ is called by DataDog when executing the custom emitter(s)
        Arguments:
        message - a JSON object representing the message sent to datadoghq
        log - the log object
        agent_config - the agent configuration object
        """

        # configuration
        if 'wf_host' not in agent_config:
            log.error('Agent config missing wf_host (the Wavefront proxy host)')
            return
        proxy_host = agent_config['wf_host']
        if 'wf_port' in agent_config:
            proxy_port = int(agent_config['wf_port'])
        else:
            proxy_port = 2878
        self.proxy_dry_run = ('wf_dry_run' in agent_config and
                              (agent_config['wf_dry_run'] == 'yes' or
                               agent_config['wf_dry_run'] == 'true'))
        if log:
            log.debug('Wavefront Emitter %s:%d ', proxy_host, proxy_port)

        if 'wf_meta_tags' in agent_config:
            self.meta_tags = [tag.strip() for tag in
                              agent_config['wf_meta_tags'].split(',')]

        try:
            # connect to the proxy
            if not self.proxy_dry_run:
                self.sock = socket.socket()
                self.sock.settimeout(10.0)
                try:
                    self.sock.connect((proxy_host, proxy_port))
                except socket.error as sock_err:
                    err_str = (
                        'Wavefront Emitter: Unable to connect %s:%d: %s' %
                        (proxy_host, proxy_port, str(sock_err)))
                    if log:
                        log.error(err_str)
                    else:
                        print err_str
                    return
            else:
                self.sock = None

            # parse the message
            if 'series' in message:
                self.parse_dogstatsd(message)

            else:
                self.parse_host_tags(message)
                self.parse_meta_tags(message)
                self.parse_collector(message)

        # pylint: disable=bare-except
        except:
            exc = sys.exc_info()
            log.err('Unable to parse message: %s\n%s',
                    str(exc[1]), str(message))

        finally:
            # close the socket (if open)
            if self.sock is not None and not self.proxy_dry_run:
                self.sock.shutdown(socket.SHUT_RDWR)
                self.sock.close()

    def parse_dogstatsd(self, message):
        """
        Parses the JSON that was sent by dogstatsd
        Arguments:
        message - a JSON object representing the message sent to datadoghq
        """

        metrics = message['series']
        for metric in metrics:
            name = metric['metric']
            jtags = metric['tags']
            tags = {}
            if jtags:
                for tag in jtags:
                    parts = tag.split(':')
                    tags[parts[0]] = parts[1]

            host_name = metric['host']
            jpoints = metric['points']
            for point in jpoints:
                tstamp = point[0]
                value = point[1]
                self.send_metric(name, value, tstamp, host_name, tags)

    # pylint: disable=too-many-arguments
    def send_metric(self, name, value, tstamp, host_name, tags):
        """
        Sends a metric to the proxy
        """

        if value is None:
            return
        skip_tag_key = None
        if tags and host_name[0] == '=' and host_name[1:] in tags:
            skip_tag_key = host_name[1:]
            host_name = tags[skip_tag_key]

        tag_str = (emitter.build_tag_string(tags, skip_tag_key) +
                   emitter.build_tag_string(self.point_tags, skip_tag_key))
        line = ('%s %s %d source="%s"%s' %
                (name, value, long(tstamp), host_name, tag_str))
        if self.proxy_dry_run or not self.sock:
            print line
        else:
            self.sock.sendall('%s\n' % (line))

    @staticmethod
    def build_tag_string(tags, skip_tag_key):
        """
        Builds a string of tag_key=tag_value ... for all tags in the tags
        dictionary provided.  If tags is None or empty, an empty string is
        returned.
        Arguments:
        tags - dictionary of tag key => tag value
        skip_tag_key - skip tag named this (None to not skip any)
        """

        if not tags:
            return ''

        tag_str = ''
        for tag_key, tag_value in tags.iteritems():
            if not isinstance(tag_value, basestring) or tag_key == skip_tag_key:
                continue
            tag_str = tag_str + ' "%s"="%s"' % (tag_key, tag_value)

        return tag_str

    @staticmethod
    def convert_key_to_dotted_name(key):
        """
        Convert a key that is camel-case notation to a dotted equivalent.
        This is best described with an example: key = "memPhysFree"
        returns "mem.phys.free"
        Arguments:
        key - a camel-case string value
        Returns:
        dotted notation with each uppercase containing a dot before
        """

        buf = []
        for char in key:
            if char.isupper():
                buf.append('.')
                buf.append(char.lower())
            else:
                buf.append(char)
        return ''.join(buf)

    # pylint: disable=too-many-locals
    def parse_collector(self, message):
        """
        Parses the JSON that was sent by the collector.
        Each metric in the metrics array is considered a metric and is sent
        to the proxy.  The metric array element is made up of:
        (0):  metric name
        (1):  timestamp (epoch seconds)
        (2):  value (assuming float for all values)
        (3):  tags (including host); all tags are converted to tags except
              hostname which is sent on its own as the source for the point.

        In addition to the metric array elements, all top level elements that
        begin with : cpu* mem* are captured and the value is sent.  These items
        are in the form of:
        {
           ...
           "collection_timestamp": 1451409092.995346,
           "cpuGuest": 0.0,
           "cpuIdle": 99.33,
           "cpuStolen": 0.0,
           ...
           "internalHostname": "mike-ubuntu14",
           ...
        }
        The names are retrieved from the JSON key name splitting the key on
        upper case letters and adding a dot between to form a metric name like
        this example: "cpuGuest" => "cpu.guest" The value comes from the JSON
        key's value.

        Other metrics retrieved:
           - ioStats group.
           - processes count
           - system.load.*

        Arguments:
        message - a JSON object representing the message sent to datadoghq
        """

        tstamp = long(message['collection_timestamp'])
        host_name = message['internalHostname']

        # cpu* mem*
        for key, value in message.iteritems():
            if key[0:3] == 'cpu' or key[0:3] == 'mem':
                dotted = 'system.' + emitter.convert_key_to_dotted_name(key)
                self.send_metric(dotted, value, tstamp, host_name, None)

        # metrics
        metrics = message['metrics']
        for metric in metrics:
            self.send_metric(
                metric[0], metric[2], long(metric[1]), '=hostname', metric[3])

        # iostats
        iostats = message['ioStats']
        for disk_name, stats in iostats.iteritems():
            for name, value in stats.iteritems():
                name = (name.replace('%', '')
                        .replace('/', '_'))

                metric_name = ('system.io.%s' % (name, ))
                tags = {'disk': disk_name}
                self.send_metric(metric_name, value, tstamp, host_name, tags)

        # count processes
        processes = message['processes']
        # don't use this name since it differs from internalHostname on ec2
        # host_name = processes['host']
        metric_name = 'system.processes.count'
        value = len(processes['processes'])
        self.send_metric(metric_name, value, tstamp, host_name, None)

        # system.load.*
        load_metric_names = ['system.load.1', 'system.load.15', 'system.load.5',
                             'system.load.norm.1', 'system.load.norm.15',
                             'system.load.norm.5']
        for metric_name in load_metric_names:
            if metric_name not in message:
                continue
            value = message[metric_name]
            self.send_metric(metric_name, value, tstamp, host_name, None)

    def parse_meta_tags(self, message):
        """
        Parses the meta dict from the JSON message, looking for any existing
        keys from the wf_meta_tags user configuration. Stores any as key
        value pairs in an instance variable
        NOTE: these are only passed on the first request (or perhaps
        only periodically?).  If nothing is in the mta dictionary then
        this function does nothing.
        Arguments:
        message - the JSON message object from the request
        Side Effects:
        self.point_tags set
        """
        if 'meta' not in message:
            return

        meta = message['meta']

        for tag in self.meta_tags:
            if tag in meta:
                self.point_tags[tag] = meta[tag]

    def parse_host_tags(self, message):
        """
        Parses the host-tags from the JSON message and stores them in an
        instance variable.
        NOTE: these are only passed on the first request (or perhaps
        only periodically?).  If nothing is in the host-tags, dictionary then
        this function does nothing.
        Arguments:
        message - the JSON message object from the request
        Side Effects:
        self.source_tags set
        self.point_tags set
        """

        if 'host-tags' not in message:
            return

        host_tags = message['host-tags']
        if not host_tags or 'system' not in host_tags:
            return

        for tag in host_tags['system']:
            self.source_tags.append(tag)
            if ':' in tag:
                parts = tag.split(':')
                k = self.sanitize(parts[0])
                v = self.sanitize(parts[1])
                self.point_tags[k] = v

    @staticmethod
    def sanitize(s):
        """
        Removes any `[ ] "' characters from the input screen
        """
        replace_map = {
            '[': '',
            ']': '',
            '"': ''
        }
        for search, replace in replace_map.iteritems():
            s = s.replace(search, replace)
        return s
