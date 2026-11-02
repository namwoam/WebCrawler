import json
import logging
from poc.distributed.app.loki import LokiReporter


def test_loki_preserves_event_fields_and_keeps_domains_out_of_labels():
    reporter = LokiReporter('http://loki:3100',capacity=2)
    record = logging.LogRecord('crawler.poc.telemetry',logging.INFO,'',0,
        json.dumps(dict(event='request.start',domain='example.org')),(),None)
    reporter.emit(record)
    payload = reporter.payload(list(reporter.records))
    stream = payload['streams'][0]
    assert 'domain' not in stream['stream']
    data = json.loads(stream['values'][0][1])
    assert data['domain']=='example.org' and data['event']=='request.start'
    assert stream['stream']['service']=='crawler-poc'
    assert stream['stream']['event']=='request.start'


def test_loki_buffers_are_bounded_and_capture_worker_warnings():
    reporter = LokiReporter('http://loki:3100',capacity=2)
    for i in range(3):
        reporter.emit(logging.LogRecord('scrapy',logging.WARNING,'',0,'warning %s',(i,),None))
    assert len(reporter.records)==2
    assert json.loads(reporter.records[-1][2])['message']=='warning 2'
    reporter.emit(logging.LogRecord('scrapy',logging.INFO,'',0,'ignore',(),None))
    assert len(reporter.records)==2
