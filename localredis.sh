#!/bin/sh

case $1 in
    'start' )
         redis-server /etc/redis.conf &
         for((i=1;i<11;i++))
             do
                 let "port=6379+$i"
                 redis-server --port $port &
             done
;;
    'stop' )
         redis-cli -p 6379 shutdown
         for((i=1;i<11;i++))
             do
                 let "port=6379+$i"
                 redis-cli -p $port shutdown
             done
;;
esac
