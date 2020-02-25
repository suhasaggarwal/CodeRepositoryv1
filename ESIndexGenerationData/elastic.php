<?php

        require 'vendor/autoload.php';

        $param = array();

        $param['hosts'] = array("wittyfeedenhancedindex.infra.cuberoot.co:9200");
    //instantiating the client
        $client = new Elasticsearch\Client($param);
        //setting some default options
        $params = [];
        $params['index'] = 'subcategorycomputeindex1';
        $params['body']['settings']['number_of_shards']   = 1;
        $params['body']['settings']['number_of_replicas'] = 0;
        //create the actual index
        $client->indices()->create($params);
?>
