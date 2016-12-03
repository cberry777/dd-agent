#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import copy
import os
import json

from tests.checks.common import AgentCheckTest

MOCK_CONFIG = {
    'init_config': {
        'min_collection_interval': 30,
        'debug': False,
        'log_at_trace': False,
        'service_tags': 'grp:celia, xx:yy',
        'leave_package_names': False
    },
    'instances': [{'host': 'stooges.com',
                'appname': 'my-app',
                'port': 8181
                   }]
}

LEVEL = logging.DEBUG

def setup_logging(logger, level=LEVEL):
    logger.setLevel(level)
    ch = logging.StreamHandler()
    logger.addHandler(ch)

tests_log = logging.getLogger('tests')
setup_logging(tests_log, level=LEVEL)
setup_logging(logging.getLogger('checks.dropwizard'), level=LEVEL)

def assertNoMetric(test, metric_name, value=None, tags=None, count=None, at_least=1, hostname=None, device_name=None, metric_type=None):
    try:
        tests_log.info("calling assertMetric for %s" % metric_name)
        test.assertMetric(metric_name, value=value, tags=tags, count=count, at_least=at_least, hostname=hostname, device_name=device_name, metric_type=metric_type)
        raise Exception("The metric (%s) should NOT be present" % metric_name)
    except AssertionError as ee:
        tests_log.debug("Expected Exception occured %s" % ee)
        pass

# -------------------------------------------------------------
# PYTHONPATH=. nosetests ./tests/checks/mock/test_dropwizard.py
# -------------------------------------------------------------
class TestCheckDropwizard(AgentCheckTest):
    CHECK_NAME = 'dropwizard'

    def __init__(self, *args, **kwargs):
        AgentCheckTest.__init__(self, *args, **kwargs)

        self.agentConfig = {
            'version': '0.1',
            'api_key': 'toto',
            'tags': ['foo:bar'],
            'hostname': 'dogstar.stooges.com'
        }

        self.data_type = 1

    def mock_fetch_dropwizard_json(self, instance):
        tests_log.debug("Fetching data from: %s" % instance)
        if (self.data_type == 1):
            return self.fetch_json('./tests/checks/fixtures/dropwizard/codahale.metrics.json')
        else:
            return []

    def _get_mocks(self):
        return {
            '_fetch_dropwizard_json': self.mock_fetch_dropwizard_json
        }

    def fetch_json(self, file):
        tests_log.debug("Fetching data from: %s" % file)
        if not file:
            raise Exception("Attempt to fetch an Empty file")
        data = {}
        if file:
            if not os.path.isfile(file) or not os.access(file, os.R_OK):
                # Don't run the check if the file is unavailable
                raise Exception("Unable to read file at %s" % file)
            else:
                with open(file, 'r') as stream:
                    data = json.load(stream)
                    stream.close()
        ## log.debug("json: %s" % data)
        return data

    def XXXtest_check_basics(self):
        tests_log.debug("RUNNING test_check_no_comments")
        self.data_type = 1
        self.run_check(MOCK_CONFIG, agent_config=self.agentConfig, mocks=self._get_mocks(), force_reload=True)
        self.print_current_state()
        self.validate_metrics()

    def test_check_leave_package_names(self):
        self.data_type = 1

        my_config = copy.deepcopy(MOCK_CONFIG)
        my_config['init_config']['leave_package_names'] = True

        self.run_check(my_config, agent_config=self.agentConfig, mocks=self._get_mocks(), force_reload=True)
        #self.print_current_state()

        tags = ['grp:celia', 'xx:yy', 'foo:bar']

        self.assertMetric('my-app.Jvm.memory.pools.Metaspace.committed', value=33685504, tags=tags)
        self.assertMetric('my-app.ch.qos.logback.core.Appender.all.count', value=14006, tags=tags)
        self.assertMetric('my-app.Jvm.memory.pools.PS-Survivor-Space.committed', value=524288, tags=tags)
        self.assertMetric('my-app.io.dropwizard.jetty.MutableServletContextHandler.3xx-responses.m1_rate', value=0.03833532101498379, tags=tags)
        self.assertMetric('my-app.io.dropwizard.jetty.MutableServletContextHandler.head-requests.max', value=0.202, tags=tags)

    def validate_metrics(self):
        tags = ['grp:celia', 'xx:yy', 'foo:bar']

        my_tags = copy.deepcopy(tags).extend([u'svc:someapp3', u'svcenv:production', u'svcver:0.0.33-snapshot', u'svciid:c352ae63'])
        self.assertMetric('my-app.routeTo.count', value=32932, tags=my_tags)

        self.assertMetric('my-app.Jvm.memory.pools.Metaspace.committed', value=33685504, tags=tags)
        self.assertMetric('my-app.Appender.all.count', value=14006, tags=tags)
        self.assertMetric('my-app.Jvm.memory.pools.PS-Survivor-Space.committed', value=524288, tags=tags)
        self.assertMetric('my-app.MutableServletContextHandler.3xx-responses.m1_rate', value=0.03833532101498379, tags=tags)
        self.assertMetric('my-app.MutableServletContextHandler.head-requests.max', value=0.202, tags=tags)

        # assert that 0 metrics are missing
        assertNoMetric(self, 'my-app.Appender.debug.count')
        assertNoMetric(self, 'my-app.Jvm.memory.pools.Metaspace.max')
        assertNoMetric(self, 'my-app.Jvm.memory.pools.Metaspace.init')
        assertNoMetric(self, 'my-app.ImmutableTopologyManager.events.max')
        assertNoMetric(self, 'my-app.MutableServletContextHandler.move-requests.max')

        # assert that m1_rate < 1e-9 are missing
        assertNoMetric(self, 'my-app.MutableServletContextHandler.4xx-responses.m1_rate')
        assertNoMetric(self, 'my-app.CloudNodeProducer.update.count')
        assertNoMetric(self, 'my-app.MutableServletContextHandler.options-requests.max')
