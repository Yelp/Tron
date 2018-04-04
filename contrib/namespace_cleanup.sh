#/bin/bash

ecosystem="stagef"

read -p "Are you at tron-$ecosystem (y/n)?" RES
echo
if [ $RES = "y" ]; then
        #load namespace from _manifest.yaml
        for namespace in $(cat /nail/tron/config/_manifest.yaml | uq | jq -r 'keys[]')
        do
                file=$(cat /nail/tron/config/_manifest.yaml | uq | jq -r .\"$namespace\")
                filename=$(basename $file)
                if [ -f "/nail/etc/services/tron/$ecosystem/$filename" ]; then
                        echo "$namespace is up to date"
                elif [ $namespace == "MASTER" ]; then
                        echo "It is MASTER namepsace"
                else
                        num_job=$(cat /nail/tron/config/$filename | uq | jq -r ".jobs | length")
                        echo "========= $filename ========="
                        cat /nail/tron/config/$filename
                        echo "============================="
                        if [ $num_job == 0 ]; then
                            echo "$namespace is left behind, deleting the namespace"
                            tronfig -d $namespace
                        else
                            echo "Can't remove the namespace since it is not empty."
                        fi
                fi
        done
else
        echo "Please change the ecosystem variable in this script or execute this script at tron-$ecosystem"
fi
