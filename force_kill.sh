#!/bin/sh

clear;ps auxww | grep -i python | grep -i splitfs | awk '{print $2}' | xargs kill -9
