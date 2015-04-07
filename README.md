# es_restart
Rolling restart for ElasticSearch Clusters

A little script which will reboot your cluster. Run from a server with ssh and http access to all the elasticsearch nodes.

Configuration is at the top of the script. Modify to suit your needs, or send a pull request with a better config approach (yaml file perhaps?)

WARNING: This is pretty alpha and it's possible I missed some edge cases. Works in production for me, but YMMV
