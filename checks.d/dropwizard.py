#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import requests
import time
import json
import copy
import re

from checks import AgentCheck
from config import _is_affirmative

EVENT_TYPE = SOURCE_TYPE_NAME = 'dropwizard'

class DropwizardError(Exception):
    pass

'''
DropwizardCheck

A dd-agent Check for the popular Java Dropwizard metrics. (see http://metrics.dropwizard.io/3.1.0/)
Also know as CodaHale metrics, after it's originator.

This check calls the standard dropwizard URL: http://localhost:8080/metrics
Although all those fields are configurable, per instance (host, port, stats_url)
This URL yields a metrics JSON response consisting of the state of the MetricRegistry at that time.

DropwizardCheck reads the metrics JSON response, parses it, and creates the corresponding DataDog metrics.
Everything is converted into a gauge, including counters. This is because of how CodaHale handles counters,
plus ease of use in Datadog dashboards. (See below)

The see /conf.d/dropwizard.yaml.example for information on all the accepted input options.

Questions or Comments: chriswberry at gmail.com

About Metric Naming
-----------------------
DropwizardCheck does a bit of manipulation on the metric names (although you can turn this off);

* Java package names are collapsed out of the metric name.
  I.e. `a.b.c.Class.method.mtype` becomes `Class.method.mtype` (where `mtype` is `max`,`min`, `mean`, etc)
* The "appname" is prepended to the metric name.
  I.e. `Class.method.mtype` becomes `appname.Class.method.mtype`

Because Dropwizard is really a framework for building webapps, most shops are likely run many different Dropwizard apps.
Often even many on the same host.

In DataDog, the first field of the metric is assumed to be the "application name".
Most off-the-shelf DataDog checks will simply set that first field to be the "application type" (e.g. "cassandra" or "mongo")
This works fine for centralized apps like DBs. But it doesn't work well for micro-services, where, in general,
most users want to see only their webapp's metrics alone, and there is no upside to lumping them together.
It especially doesn't work well in DataDog's Infrastructure View,
where all webapps on the box would show up grouped into a single "dropwizard".

So rather than roll all those metrics into a single "dropwizard" application -- it is a
better idea to supply an "appname" per instance. This will prefix the "appname" as the first field of the metric,
instead of "dropwizard". Which makes things in the DataDog UI much easier.

About Metric Tagging
---------------------------
You can supply metric tags at all three levels of the configuration; agent_config, init_config, and instance.

In addition, DropwizardCheck can also do a bit of magic metric tagging for you.

If the metric name contains a field like this; .(x=y,a=b). , then that field is extracted and tags are created (`a:b` and `x:y`).

Let's look at a real example. Assume that you've created the following metric inside your application; `com.x.y.ServletHandler.(ec=listings,sr=find).requests.count`
DropwizardCheck will look for any matching fields with; `.(key=value).`, and, if found, will extract that field, and use it to create DataDog tags.
Thus, for our example, your metric will become; `appname.ServletHandler.requests.count` with the following tags applied; `{'ec:listings', 'sr:find'}`

About "Zero values"
---------------------------
CodaHale will report all metrics in the underlying MetricRegistry -- even if they are zero or appeared once 30 days ago.
This can lead to a lot of confusion.

Timers that measure very rare events may contain samples that are so far back in time that they cause more
confusion than anything else.  The classic example is an Error metric that occurred once at startup, due to, say, a long
out-of-SLA elapsed time because nothing was warm -- the Counter would report that one measurement forever (until
next restart) suggesting that the app is unhealthy or slow, even though it's really not.  To work around this,
we omit histogram-based timer measurements from the metrics output when a particular timer hasn't received any
recent events.

A timer that receives a single event takes 15 minutes for the "one minute rate" to drop < 1e-7.  When
more than one event is received it'll take a little longer than 15 minutes, but not too much longer.

Similarly, we do not report zero metrics. Zero means that metric has never occurred during the run,
and there is little sense in reporting something that didn't happen, over and over.
Not to mention, we pay for every custom metric in DataDog, regardless if they are always zero.

About CodaHale Counts
----------------------------
The default form of Counters from CodaHale are monotonically increasing numbers
But, we can NOT use monotonic_count in the dd-agent -- because a  monotonic_count must, ingeneral, ALWAYS increase (in DD)
And will be stored as Zero if a number less than the current total is submitted (in DD)
Thus, we would miss initial numbers after a service restart -- which restarts the CodaHale Counters over at Zero
Per DataDog support --
 The counter is only 0 when *consecutive* values are not increasing.
 So the following value pattern: 0 ; 1000 ; 1200 ; 400 (restart) ; 600; 800​; 1200
 will result in the graph (as count): nothing yet; 1000 ; 200 ; 0 ; 200; 400

 NOTE: this still produces erroneous results -- since the 400 is missed -- and the overall total will be wrong
'''

class EncodedTagsProcessor(object):
    # There must be at least one = sign within the ()
    BETWEEN_PARANS_REGEX = '\.\((\S*=\S*)\)\.'
    PATTERN = re.compile(BETWEEN_PARANS_REGEX)

    def process_tags_from_metric(self, full_metric, log, tag_prefix=None):
        self.trace(log, "full_metric: %s", full_metric)

        metric = full_metric
        tags = []
        if (self._metric_is_not_empty(metric)):
            match = self.PATTERN.search(full_metric)
            if match:
                self.trace(log, "EncodedTagsProcessor: MATCH: match %s", match.groups())
                field = match.group(1)

                tags = self._process_tags_field(field, log, tag_prefix)

                # delete from the metric
                metric = metric[0:match.start()] + "." + metric[match.end():]
            else:
                self.trace(log, "SKIPPING: (%s) no fields match pattern", full_metric)
        else:
            self.trace(log, "SKIPPING %s", full_metric)

        self.trace(log, "metric %s, tags %s", metric, tags)
        return metric, tags

    def _metric_is_not_empty(self, metric):
        return ((metric is not None) and (metric.strip()))

    def _process_tags_field(self, field, log, tag_prefix):
        tags = []
        kvs = field.strip().split(',')
        self.trace(log, "kvs: %s", kvs)

        for kv in kvs:
            kvsplit = kv.strip().split('=')
            key = (tag_prefix + '_' + kvsplit[0]) if (tag_prefix is not None) else kvsplit[0]
            tags.append(key + ":" + kvsplit[1])
        self.trace(log, "tags: %s", tags)

        return tags

    def trace(self, log, fmt, *arg):
        if log:
            log.debug(fmt % arg)

#################################################################
class DropwizardCheck(AgentCheck):
    # All metrics will be prefixed with this field. Unless an "appname" is forun in the instance config
    DEFAULT_METRIC_PREFIX = 'dropwizard'

    # Defaults to "http://localhost:8080/metrics"
    # MAy be overriden in the instance config
    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 8080
    DEFAULT_STATS_URL = "/metrics"

    # metrics with these suffixes will be ignored
    DEFAULT_METRIC_TYPE_BLACKLIST = ['.tps15', '.p75', '.p98']

    # Timeout to call http (in seconds)
    DEFAULT_TIMEOUT = 0.25

    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)
        self.log.debug("DropwizardCheck::agentConfig: %s\ninit_config: %s" % (agentConfig, init_config))

        self.log_each_metric = self.init_config.get('log_each_metric', False)
        self.log_at_trace = self.init_config.get('log_at_trace', False)
        debug = self.init_config.get('debug', False)
        if debug:
            self.log.setLevel(logging.DEBUG)

        self.starts_with_cap_pattern = re.compile("\.([A-Z][\w]*)\.")

        self.http_timeout = self.init_config.get('http_timeout', self.DEFAULT_TIMEOUT)
        self.metric_type_blacklist = init_config.get('metrictype_blacklist', self.DEFAULT_METRIC_TYPE_BLACKLIST)
        self.leave_package_names = _is_affirmative(init_config.get('leave_package_names', False))

        self.service_tags = self._clean_tags(init_config.get('service_tags', None))
        self.agent_tags = self._clean_tags(agentConfig.get('tags'))
        self.log.debug("agent_tags: %s service_tags %s" % (self.agent_tags, self.service_tags))

        self.encoded_tags_processor = EncodedTagsProcessor()

    def check(self, instance):
        dropwizard_json = self._fetch_dropwizard_json(instance)
        if dropwizard_json:
            self._process_dropwizard_json(dropwizard_json, instance)

    def get_encoded_tags_processor(self):
        return self.encoded_tags_processor

    def _fetch_dropwizard_json(self, instance):
        url = 'undefined'
        try:
            host = instance.get('host', self.DEFAULT_HOST)
            port = instance.get('port', self.DEFAULT_PORT)
            stats_url = instance.get('stats_url', self.DEFAULT_STATS_URL)

            url = "http://" + host + ":" + str(port) + stats_url

            self.log.debug("Fetching dropwizard data from: %s" % url)
            resp = requests.get(url, timeout=self.http_timeout)  # timeout after 100ms
            resp.raise_for_status()

            return resp.json()

        except Exception as e:  # Log and move on....
            raise DropwizardError("%s Could not fetch: for %s (%s)" % (repr(e), url, instance))

    def process_counters(self, section_data, tags, appname):
        '''
          "counters": {
            "io.dropwizard.jetty.MutableServletContextHandler.active-dispatches": {
              "count": 0
            },
            "io.dropwizard.jetty.MutableServletContextHandler.active-requests": {
              "count": 0
            }
          },
        '''
        self.trace("COUNTERS: %s", section_data)
        for base_metric_name, metric_data in section_data.iteritems():
            if metric_data['count'] is 0:
                self.log.debug("SKIPPING ZERO METRIC: %s" % base_metric_name)
                continue

            metric = base_metric_name + '.count'
            mtags = copy.deepcopy(tags)
            self._process_metric(appname, metric, metric_data['count'], mtags)

    def process_gauges(self, section_data, tags, appname):
        '''
          "gauges": {
            "io.dropwizard.jetty.MutableServletContextHandler.percent-4xx-15m": {
              "value": 0.019564512218014994
            },
            "io.dropwizard.jetty.MutableServletContextHandler.percent-4xx-1m": {
              "value": 0.003530850874892612
            }
          },
        '''
        self.trace("GAUGES: %s", section_data)
        for base_metric_name, metric_data in section_data.iteritems():
            # the value may be; -1, 0, int, float, string
            # we need to skip -1 & 0 metrics
            value = metric_data['value']
            self.trace("NAME: %s VALUE: %s", base_metric_name, value)

            if not self._is_primitive(value):
                self.log.debug("SKIPPING STRING METRIC: %s" % base_metric_name)
                continue

            if value in [-1, 0]:
                self.log.debug("SKIPPING 0 or -1 METRIC: %s" % base_metric_name)
                continue

            metric = base_metric_name
            mtags = copy.deepcopy(tags)

            self._process_metric(appname, metric, value, mtags)

    def process_histograms(self, section_data, tags, appname):
        '''
          "histograms": {},
        }
        '''
        # Note -- are there any no 'units' in histogram JSON
        self._process_reservoir_metric(section_data, tags, 'HISTOGRAMS', [], appname)

    def process_meters(self, section_data, tags, appname):
        '''
          "meters": {
            "ch.qos.logback.core.Appender.all": {
              "count": 130,
              "m15_rate": 0.33050995296474994,
              "m1_rate": 0.008913390064755949,
              "m5_rate": 0.10601442238857867,
              "mean_rate": 0.1705098859093358,
              "units": "events/second"
            }
          },
        }
        '''
        self._process_reservoir_metric(section_data, tags, 'METERS', ['units'], appname)

    def process_timers(self, section_data, tags, appname):
        '''
          "timers": {
            "com.foo.web.InquiriesResource.findByEmail": {
              "count": 8,
              "max": 0.12919699,
              "mean": 0.006554913354028336,
              "min": 0.0008676440000000001,
              "p50": 0.008963556000000001,
              "p75": 0.009144276,
              "p95": 0.009144276,
              "p98": 0.009144276,
              "p99": 0.009144276,
              "p999": 0.12919699,
              "stddev": 0.005611150805755341,
              "m15_rate": 0.005474707899477395,
              "m1_rate": 0.0012658831602204207,
              "m5_rate": 0.007079839429920503,
              "mean_rate": 0.010663674994514599,
              "duration_units": "seconds",
              "rate_units": "calls/second"
            }
        '''
        self._process_reservoir_metric(section_data, tags, 'TIMERS', ['duration_units', 'rate_units'], appname)

    def _process_reservoir_metric(self, section_data, tags, section_name, types_to_skip, appname):
        self.trace("SECTION: %s: %s", section_name, section_data)
        for base_metric_name, metric_data in section_data.iteritems():
            # skip metrics
            if self._skips_reservoir(metric_data, base_metric_name):
                continue

            for metric_type, value in metric_data.iteritems():
                self.trace("NAME: %s TYPE %s VALUE: %s", base_metric_name, metric_type, value)
                if metric_type in types_to_skip:
                    self.trace("SKIPPING TYPE: %s %s", base_metric_name, metric_type)
                    continue

                metric = base_metric_name + '.' + metric_type
                mtags = copy.deepcopy(tags)
                self._process_metric(appname, metric, value, mtags)

    def _skips_reservoir(self, metric_data, base_metric_name):
        '''
         Timers that measure very rare events may contain samples that are so far back in time that they cause more
         confusion than anything else.  The classic example is an event that occurred once at startup and had a long
         out-of-SLA elapsed time because nothing was warm: the jvm would report that one measurement forever (until
         restart) suggesting that the app is unhealthy and slow, even though it's really not.  To work around this,
         omit histogram-based timer measurements from the metrics output when a particular timer hasn't received any
         recent events.

         A timer that receives a single event takes 15 minutes for the "one minute rate" to drop < 1e-7.  When
         more than one event is received it'll take a little longer than 15 minutes, but not too much longer.
        '''
        if metric_data['count'] is 0:
            self.log.debug("SKIPPING 0 METRIC: %s" % base_metric_name)
            return True
        m1rate = metric_data.get('m1_rate', None)

        self.trace("###### m1rate %s", m1rate)
        if (m1rate is not None) and (m1rate < 1.0e-7):
            self.log.debug("SKIPPING (m1rate < 1e-7) METRIC: %s %s" % (base_metric_name, m1rate))
            return True
        return False

    # Note: see https://github.com/dropwizard/metrics/blob/dff1a69b3a0824ff445492777052ea0417b9c5cf/metrics-json/src/main/java/com/dropwizard/metrics/json/MetricsModule.java
    #       for details on JSON formats
    METHOD_MAP = {u'counters' : process_counters,
                  u'gauges' : process_gauges,
                  u'histograms' : process_histograms,
                  u'meters' : process_meters,
                  u'timers' : process_timers}

    def _process_dropwizard_json(self, dropwizard_json, instance):
        ''' Main data-processing loop.
        http json data looks like this
        {
          "version": "3.0.0",
          "gauges": {},
          "counters": {},
          "histograms": {},
          "meters": {},
          "timers": {}
        }
        '''

        appname = instance.get('appname', self.DEFAULT_METRIC_PREFIX)

        tags = self._clean_tags(instance.get('instance_tags', None))
        tags = self._extend_with_addtl_tags(tags, self.service_tags)
        tags = self._extend_with_addtl_tags(tags, self.agent_tags)

        for key, section in dropwizard_json.iteritems():
            try:
                if key in self.METHOD_MAP:
                    self.METHOD_MAP[key](self, section, tags, appname)
            except:
                # Log and move on....
                self.log.exception("Could not process line. For instance: %s" % (instance))

    def _process_metric(self, appname, metric, value, tags):
        if self._skips_metric(metric):
            self.log.debug("SKIPPING Metric (blacklisted type) : %s" % metric)
            return

        metric = self._process_metricname(metric)
        if metric is None:
            return

        metric, addtl_tags = self._process_tags(metric, appname)
        if metric is None:
            return
        if addtl_tags:
            tags = self._extend_with_addtl_tags(tags, addtl_tags)

        # Because of how the DataDog UI (Infrastructure View) is setup -- the first "field" is always assumed to be the app's name
        #  So we accommodate that, by doing it explicitly
        metric = appname + "." + metric

        self._process_gauge(metric, value, tags)

    def _skips_metric(self, metric):
        return self._is_blacklisted_metric_type(metric)

    def _is_blacklisted_metric_type(self, metric):
        if self.metric_type_blacklist is not None:
            for type in self.metric_type_blacklist:
                if metric.endswith(type):
                    return True
        return False

    def _process_metricname(self, metric):
        if metric is None:
            return None

        # we pass thru all JVM metrics as is (except change the name to match our convention)
        if metric.startswith('jvm'):
            metric = metric.replace('jvm', 'Jvm')
            return metric

        # Remove the java package name up front
        if not self.leave_package_names:
            match = self.starts_with_cap_pattern.search(metric)
            if match:
                self.trace("MATCH: match %s", match.groups())
                # Skip the first matched "."
                metric = metric[(match.start() + 1):]

        return self._cleanup_metric(metric)

    def _cleanup_metric(self, metric):
        metric = metric.replace('..', '.')
        if metric.startswith('.'):
            metric = metric[1:]
        return metric

    def _process_tags(self, metric, appname):
        log = self.log if self.log_at_trace else None
        addtl_tags = []

        metric, addtl_tags1 = self.encoded_tags_processor.process_tags_from_metric(metric, log)
        if metric is None:
            return None, []
        addtl_tags.extend(addtl_tags1)

        self.trace(">> metric %s, addtl_tags: %s", metric, addtl_tags)
        return metric, addtl_tags

    def _get_tag_prefix(self, appname):
        return appname.split('-')[0]

    # Python reads in the agent_tags as a list of chars. Go figure.
    def _clean_tags(self, tags_in):
        tags_out = []
        if tags_in:
            sss = ''.join(tags_in)
            tags_out = sss.strip().split(', ')
        return tags_out

    def _process_gauge(self, metric, value, tags):
        if self.log_each_metric or self.log_at_trace:
            self.log.info("%%%%%% ADDING gauge **[[ %s ]]** %s %s" % (metric, value, tags))
        self.gauge(metric, value, tags=tags)

    def _process_counter(self, metric, value, tags):
        ival = int(float(value)) if self._isfloat(value) else int(value)
        if self.log_each_metric or self.log_at_trace:
            self.log.info("%%%%%% ADDING counter **[[ %s ]]** %s %s" % (metric, ival, tags))
        self.count(metric, ival, tags=tags)

    def _extend_with_addtl_tags(self, tags, addtl_tags):
        if addtl_tags:
            for tag in addtl_tags:
                tags.append(tag)
        self.trace("global_tags: %s", tags)
        return tags

    def _isfloat(self, value):
        # NOTE: value here is a string, so we have to test if it can be cast to a float....
        try:
            float(value)
            return True
        except:
            return False

    def _is_primitive(self, value_element):
        return (type(value_element) in [int, float, long])

    def trace(self, fmt, *arg):
        if self.log_at_trace:
            self.log.debug(fmt % arg)


# ---------------------------
# Code for the MAIN below
# ---------------------------
# PYTHONPATH=. python checks.d/dropwizard.py
#
def process_check(check):
    time_start = time.clock()
    check.check(instance)
    metrics = check.get_metrics()
    print 'Metrics: %s' % (json.dumps(metrics, indent=4, sort_keys=True))
    print 'NUM Metrics: %s' % (len(metrics))
    time_elasped = time.clock() - time_start
    log.debug("Processing dropwizard took: %s" % time_elasped)

def setup_logger(name):
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    log.addHandler(ch)
    return log

if __name__ == '__main__':
    import traceback

    from config import initialize_logging
    initialize_logging('collector')

    log = setup_logger('checks.dropwizard')
    setup_logger('aggregator')

    agentConfig = {
        'version': '0.1',
        'api_key': 'toto',
        'tags': 'foo:bar, blah:blah'
    }
    check, instances = DropwizardCheck.from_yaml('./tests/checks/fixtures/dropwizard/dropwizard.1.yaml', agentConfig)

    try:
        for instance in instances:
            process_check(check)

    except Exception as e:
        print "Whoops something happened {0}".format(traceback.format_exc())
    finally:
        check.stop()
