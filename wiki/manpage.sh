#!/bin/sh

cat << "xxEOFxx"
#summary Wiki-fied version of the s3backer man page
#labels Featured

{{{
xxEOFxx

groff -r LL=131n -r LT=131n -Tlatin1 -man ../trunk/s3backer.1 | sed -r 's/.\x08(.)/\1/g'

cat << "xxEOFxx"
}}}
xxEOFxx
