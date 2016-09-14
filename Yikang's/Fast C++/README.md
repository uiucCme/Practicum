itch5.0 to csv
========

A parser for the NASDAQ ITCH5 Market Information, credit to drhurd/itch4csv



Something to note
====
Your processor must be little-endian for this to work, because the numbers are read in byte-by-byte.  Endian independence is coming shortly.



Usage: ./test [input NASDAQ file] [output CSV file]

Compile with: g++ -O3 -o test main.cpp