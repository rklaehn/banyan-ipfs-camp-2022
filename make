#!/bin/bash
pandoc -f markdown -t slidy -i --standalone --embed-resources -o output/banyan.html banyan.md
pandoc -f markdown -t beamer -i -o output/banyan.pdf banyan.md
