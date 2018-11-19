/usr/local/lib/node_modules/elasticdump/bin/elasticdump --input=http://172.16.105.229:9200/livedmpindex --output=http://172.16.105.231:9200/enhanceduserdatabeta1 --type=data --searchBody='{ "query" : {
        "range" : {
            "request_time" : {
                "from" : "now-5m",
                "to" : "now"
            }
        }
    }
}'

