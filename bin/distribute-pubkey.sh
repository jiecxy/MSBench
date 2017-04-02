#!/usr/bin/env bash
#1.gen key
if [ ! -f ~/.ssh/id_rsa.pub ]; then
ssh-keygen -t rsa
fi
#2.set user pass
USER=kangsiqi
PASSWD=123456
#3.distribute
for ip in $(cat iplist)
do
        expect ssh.exp  $USER $PASSWD $ip
done