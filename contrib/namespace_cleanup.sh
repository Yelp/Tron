#/bin/bash

ecosystem="stagef"

read -p "Are you at tron-$ecosystem (y/n)?" RES
echo
if [ $RES = "y" ]; then
        for file in /nail/tron/config/*
        do
                filename="$(basename $file)"
                namespace="$(basename $file .yaml)"
                if [ -f "/nail/etc/services/tron/$ecosystem/$filename" ]; then
                        echo "$namespace is up to date"
                else
                        echo "$namespace is left behind, delete the namespace"
                        tronfig -d $namespace
                fi
        done
else
        echo "Please change the ecosystem variable in this script or execute this script at tron-$ecosystem"
fi
