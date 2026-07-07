#!/usr/bin/bash
# shellcheck disable=SC2034  # consumed by bin/sync_truenas_servers (and lib/common.bash), which sources this file
declare EMAIL_TO="you@example.com"
declare SSH_CONFIG_FILE="/path/to/ssh/config"
declare LOG_RETENTION_DAYS="365"
declare DUMP_RETENTION_DAYS="365"
