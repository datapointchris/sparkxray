#!/bin/sh
# Run one command in a fixture container as root, then give /out and /data to the host user
# named by $OWNER, whatever the command's outcome. The Spark images run as a user whose uid owns
# nothing on the host, so their output would otherwise be undeletable there.
status=0
"$@" || status=$?
chown -R "$OWNER" /out /data
exit "$status"
