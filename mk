#!/bin/bash
pandoc \
  -f markdown \
  -t revealjs \
  -V theme=white -i --slide-level=2 \
  --standalone --embed-resources \
  -o target/banyan.html \
  --verbose \
  banyan.md
