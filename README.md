# pytroll-run-command

This module listens to messages from various other modules. When a matching message is received this can trigger all kind of commmand line actions.
Mostly it starts scripts that do processing to images. But it can also do rsync. In short: what you can do on command line can be started from this module.
It can also publish messages of completed results. This is done by configured matching of the logs.

[![coverage report](https://gitlab.met.no/obs-fm-pytroll/pytroll-run-command/badges/run-all/coverage.svg)](https://gitlab.met.no/obs-fm-pytroll/pytroll-run-command/-/commits/run-all) [![pipeline status](https://gitlab.met.no/obs-fm-pytroll/pytroll-run-command/badges/run-all/pipeline.svg)](https://gitlab.met.no/obs-fm-pytroll/pytroll-run-command/-/commits/run-all)
