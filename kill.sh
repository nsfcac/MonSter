#!/bin/bash

kill $(ps aux | grep '[m]onster.py' | awk '{print $2}')