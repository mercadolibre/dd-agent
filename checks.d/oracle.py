import subprocess
import os
import sys
import re
import traceback
import signal
from checks import AgentCheck

GAUGE = "gauge"
RATE = "rate"

QUERIES_COMMON = [
('oracle.compute.cpu_per_sec', "select METRIC_NAME,VALUE from SYS.V_$SYSMETRIC where METRIC_NAME like 'CPU Usage Per Sec' AND INTSIZE_CSEC = (select max(INTSIZE_CSEC) from SYS.V_$SYSMETRIC)", GAUGE),
    ('oracle.compute.cpu_per_txn', "select METRIC_NAME,VALUE from SYS.V_$SYSMETRIC where METRIC_NAME like 'CPU Usage Per Txn' AND INTSIZE_CSEC = (select max(INTSIZE_CSEC) from SYS.V_$SYSMETRIC)", GAUGE),
    ('oracle.compute.db_cpu_ratio', "select METRIC_NAME,VALUE from SYS.V_$SYSMETRIC where METRIC_NAME like 'Database CPU Time Ratio' AND INTSIZE_CSEC = (select max(INTSIZE_CSEC) from SYS.V_$SYSMETRIC)", RATE),
    ('oracle.compute.db_wait_ratio', "select METRIC_NAME,VALUE from SYS.V_$SYSMETRIC where METRIC_NAME like 'Database Wait Time Ratio' AND INTSIZE_CSEC = (select max(INTSIZE_CSEC) from SYS.V_$SYSMETRIC)", RATE),
    ('oracle.compute.exec_per_sec', "select METRIC_NAME,VALUE from SYS.V_$SYSMETRIC where METRIC_NAME like 'Executions Per Sec' AND INTSIZE_CSEC = (select max(INTSIZE_CSEC) from SYS.V_$SYSMETRIC)", GAUGE),
    ('oracle.compute.exec_per_txn', "select METRIC_NAME,VALUE from SYS.V_$SYSMETRIC where METRIC_NAME like 'Executions Per Txn' AND INTSIZE_CSEC = (select max(INTSIZE_CSEC) from SYS.V_$SYSMETRIC)", GAUGE),
    ('oracle.compute.user_txn', "select METRIC_NAME,VALUE from SYS.V_$SYSMETRIC where METRIC_NAME like 'User Transaction Per Sec' AND INTSIZE_CSEC = (select max(INTSIZE_CSEC) from SYS.V_$SYSMETRIC)", GAUGE),
    ('oracle.compute.resp_time_per_txn', "select METRIC_NAME,ROUND((VALUE / 100),2) as VALUE from SYS.V_$SYSMETRIC where METRIC_NAME like 'Response Time Per Txn' AND INTSIZE_CSEC = (select max(INTSIZE_CSEC) from SYS.V_$SYSMETRIC)", GAUGE),
    ('oracle.compute.sql_resp_time', "select METRIC_NAME,ROUND((VALUE / 100),2) as VALUE from SYS.V_$SYSMETRIC where METRIC_NAME like 'SQL Service Response Time' AND INTSIZE_CSEC = (select max(INTSIZE_CSEC) from SYS.V_$SYSMETRIC)", GAUGE),
    ('oracle.compute.sessions', "select 'sessions',count(*) from v$session where username is not null", GAUGE)
    ]

class Oracle(AgentCheck):
    def check(self, instance):
        sid, tags, options = self._get_config(instance)

        if tags is None:
            tags = []

        if sid is None or sid is "":
            process = subprocess.Popen(['ps', '-ef'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
            stdout = process.communicate()[0]
            sid_regex = re.compile("pmon_(.*)\s")
            sids = sid_regex.findall(stdout)

            for sid in sids:
                db = self._connect(sid)
                db_tags = tags + ["db:%s" % sid[:-1], "db_instance:%s" % sid]
                self._collect_metrics(db, db_tags, options)

        else:
            db = self._connect(sid)
            self._collect_metrics(db, tags, options)

    def _get_config(self, instance):
        sid = instance.get('sid', None)
        tags = instance.get('tags', [])
        options = instance.get('options', {})

        return sid, tags, options

    def _connect(self, sid):
        try:
            import cx_Oracle
        except ImportError:
            raise Exception("Cannot import cx_Oracle module. Check the instructions to install this module at https://app.datadoghq.com/account/settings#integrations/oracle")
        if sid is not None:
            os.environ['ORACLE_SID'] = sid
            db = cx_Oracle.connect(mode = cx_Oracle.SYSDBA)
            signal.signal(signal.SIGCHLD, signal.SIG_DFL)
            self.log.debug("Connected to Oracle")
            return db

    def _collect_metrics(self, db, tags, options):
        queries = QUERIES_COMMON
        for metric_name, query, metric_type in queries:
            value = self._collect_scalar(query, db)
            if value is not None:
                if metric_type == RATE:
                    self.rate(metric_name, value, tags=tags)
                elif metric_type == GAUGE:
                    self.gauge(metric_name, value, tags=tags)

    def _collect_scalar(self, query, db):
        self.log.debug("Collecting data with %s" % (query))
        try:
            cursor = db.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            del cursor
            if result is None:
                self.log.debug("%s returned None" % query)
                return None
            self.log.debug("Collecting done, value %s" % result[1])
            return float(result[1])
        except Exception:
            self.log.exception("Error while running %s" % query)
