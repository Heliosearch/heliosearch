# SIREn Documentation Build Instructions

## Introduction

Basic steps:
  0) Install Asciidoc
  1) Build the HTML output
  2) Build the PDF output
   
## Install Asciidoc

This module requires the asciidoc tool. You can install it by typing:

  $ sudo apt-get install asciidoctor
  
  OR for OSX 
  
  $ sudo port install asciidoc
  
   
## Build the Wordpress output

To build the Wordpress output

  $ make wordpress

It will generate a chunked PHP/HTML version of the manual in the directory "./target/wp_chunked/".
Given that the generated HTML contains PHP instructions, the Apache server must be configured to
run .html pages as .php files. To do this, add the following line in the .htaccess:

    AddType application/x-httpd-php .php .html

## Build the HTML output

You can build the HTML output by typing:
   
   $ make html
   
It will generate a chunked HTML version of the manual in the directory "./target/chunked/"
   
## Build the PDF output

To build the pdf output

  $ make pdf
  
It will generate a PDF version of the manual located at "./target/siren-manual.pdf"

- - -

Copyright (c) 2014, Sindice Limited. All Rights Reserved.

