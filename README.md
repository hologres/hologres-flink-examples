# Hologres-Flink Quickstart

Examples to show how to use Hologres connector in Flink.

Examples are written as unit tests in `/src/test`.

## Documentation

Documentation of quickstart project can be found on [Hologres documentation website](http://docs.hologres.io/en/latest/quickstart/quickstart-flink.html).

For detailed documentation of how to use Hologres and Flink together, please see [Hologres sink](https://hologres.readthedocs.io/en/latest/data_load/flink.html) and [Hologres source and temporal table/function](https://hologres.readthedocs.io/en/latest/data_read_and_unload/flink.html).
 
## Pre-requisite

Since Hologres connector is not open-sourced and available on mvn central yet, 
please contact Hologres team to get the connector jar, put it into `/libarary` dir, and 
run the `install-hologress-connector-to-local-mvn.sh` first to install the jar into your 
local mvn repository. We are sorry for the inconvenience, and the connector will be open-sourced 
soon.



