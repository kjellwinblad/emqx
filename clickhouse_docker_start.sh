#!/bin/sh


docker run -d -p 18123:8123 -p19000:9000 --name some-clickhouse-server --ulimit nofile=262144:262144 -v "`pwd`/.ci/docker-compose-file/clickhouse/users.xml:/etc/clickhouse-server/users.xml" -v "`pwd`/.ci/docker-compose-file/clickhouse/config.xml:/etc/clickhouse-server/config.xml" clickhouse/clickhouse-server



TESTCASE=send SUITE=lib-ee/emqx_ee_bridge/test/emqx_ee_bridge_clickhouse_SUITE.erl make ct-suite




export LOGLEVEL=debug
