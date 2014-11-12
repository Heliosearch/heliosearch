#!/bin/bash

srcdir=$1
imgdir=$2

echo "${srcdir}"
echo "${imgdir}"

mkdir -p "${imgdir}"
find "${srcdir}"/*/images -type d -print | while read i; do
  cp -fr "${i}"/* "${imgdir}"
done
