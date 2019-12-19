#!/usr/bin/env bash

clear
sed -i '/.*/d' ./*.log
termite -e './broker.py' &
termite -e './bob.py' &
termite -e './joe.py' &
termite -e './linda.py' &
