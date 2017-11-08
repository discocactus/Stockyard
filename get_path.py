#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys

print("\nいま実行中のスクリプトファイルの相対パスを表示します。\n")
print("__file__ :%s" %  __file__) 
print("sys.argv[0] :%s" %  sys.argv[0])
print("\n======================================================================================")


print("\nいま実行中のスクリプトファイルの配置先ディレクトリの相対パスを表示します。\n")
print("os.path.dirname(__file__) : %s" % os.path.dirname(__file__))
print("\n======================================================================================")


sql_file_path = os.path.join(os.path.dirname(__file__), 'test.sql')
print("\nいま実行中のスクリプトファイルの配置先ディレクトリにあるtest.sqlファイルの相対パスを表示します。\n")
print("os.path.join(os.path.dirname(__file__), 'test.sql') : %s" % sql_file_path)
print("\n======================================================================================")

print("\nいま実行中のスクリプトファイルのファイル名を表示します。\n")
print("os.path.basename(__file__) :%s" % os.path.basename(__file__))

print("\n======================================================================================")
print("\nいま実行中のスクリプトファイルの絶対パスを表示します。\n")
print("os.path.abspath(__file__) :%s" % os.path.abspath(__file__))


print("\n======================================================================================")
print("\nいま実行中のスクリプトファイルの配置先ディレクトリの絶対パスを表示します。\n")
print("os.path.abspath(os.path.dirname(__file__)) :%s" % os.path.abspath(os.path.dirname(__file__)))


print("\n======================================================================================")
print("\nいま実行中のスクリプトファイルの配置先ディレクトリにあるtest.sqlファイルの絶対パスを表示します。\n")
print("os.path.abspath(test.sql) :%s" % os.path.abspath("test.sql"))

print("\n======================================================================================")
print("\nいまスクリプトを実行しているカレントディレクトリの絶対パスを表示します。\n")
print("os.getcwd() : %s \n" % os.getcwd())


print("\n======================================================================================")
print("\ntest.sql の ファイルを１行づつ読み込んで、表示します。\n")

f = open(sql_file_path, 'r')
for line in f:
    print(line)

print("\n")