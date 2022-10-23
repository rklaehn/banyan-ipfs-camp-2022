#!/bin/bash
pandoc -f markdown -t slidy -i --standalone --embed-resources -o target/banyan.html banyan.md
pandoc -f markdown -t beamer -i -o target/banyan.pdf banyan.md
