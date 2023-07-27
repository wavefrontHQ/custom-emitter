> **Warning**
>
> VMware has ended active development of this project. this repository will no longer be updated.

# About

This is a [custom emitter](https://github.com/DataDog/dd-agent/wiki/Using-custom-emitters) for the DataDog Agent forwarder which will send metrics from the DataDog Collector and DogStatsD to Wavefront via a [Proxy](https://github.com/wavefrontHQ/install)

# Using

These instructions assume that the DataDog Agent is already installed on the machine you want to emit data from, and that it is configured to use the Forwarder as per the [default configuration](https://github.com/DataDog/dd-agent/wiki/Agent-Architecture).

1. Place `wavefrontEmitter.py` somewhere on the file system, e.g. `/opt/wavefront`
2. Edit `/etc/dd-agent/datadog.conf` and add these 2 lines:

  ```
  wf_host: proxy-host
  custom_emitters: /opt/wavefront/wavefrontEmitter.py
  ```
  
  where `proxy-host` is the hostname of the machine running the Wavefront Proxy.
3. `sudo /etc/init.d/datadog-agent restart`
