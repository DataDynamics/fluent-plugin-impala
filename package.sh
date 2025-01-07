#!/bin/sh

systemctl stop fluentd
/opt/fluent/bin/gem build fluent-plugin-impala.gemspec
/opt/fluent/bin/gem install fluent-plugin-impala-0.1.0.gem
ls -lsa /opt/fluent/lib/ruby/gems/3.2.0/gems/fluent-plugin-impala-0.1.0
