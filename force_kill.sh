#!/bin/sh

ps auxww | grep -i python | grep -i splitfs | awk '{print $2}' | xargs kill -9
umount -f mountpoint
