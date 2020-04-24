#!/usr/bin/env bash

go build -v -i || exit 1

 ./sbotcli --unixsock '' --addr hermiesroom.staltz.com:8008 --remoteKey 'W18NyiHagiiVE4/jm2sxzqQqIwmUw2p9pKZ0lHH9OXA=' tunnel
#./sbotcli --unixsock '' --addr slackware.server.ky:8008 --remoteKey 'zwWhxxCZnOGuqpg7jH8SnfQK0ykR3tpmcQvdnKss4ww=' tunnel

# :SSB+Room+PSK3TLYC2T86EHQCUHBUHASCASE18JBV24=
# ??

