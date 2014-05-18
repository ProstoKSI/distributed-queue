#!/bin/bash
pylint distributed_queue --rcfile=pylint.rc | tee pylint.log || true
