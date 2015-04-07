// ==== CONFIG ==== //


// location of the elasticsearch binary
//esBinary = 'elasticsearch-1.4.4/bin/elasticsearch'
esBinary = 'elasticsearch/bin/elasticsearch'

// location of the elasticsearch config file
esConfig = '/etc/elasticsearch/elasticsearch.yml'

// location for the elasticsearch PID file
esPID = 'elasticsearch.pid'

// url used to make initial connection
clusterUrl = 'http://sssvcsav01.umasscs.net:9200'

// user to login to nodes with
// Note: you will need to have ssh keys setup for autologin
esUser = 'logstash'


// ==== CODE ==== //

// Globals
var async = require('async')
var request = require('request')
var Backoff = require('backo')
var sshclient = require('node-sshclient')

var nodes = []
var nodeCount = 0


// get list of nodes in the cluster
function clusterNodeURLs(callback, clusterUrl) {
	var url = clusterUrl + '/_nodes'
	request({uri:url, json:true, proxy:null}, function(error, response, body) {
		if (error || response.statusCode != 200) {
			console.log('Cannot connect to cluster: ' + clusterUrl)
			console.log(error)
			callback(error)
		} else {
			var nodes = []
			for (var node in body.nodes) {
				var address = body.nodes[node].http_address
				address = address.split('/')[1].replace(']','')
				var nodeObject = {
					name: body.nodes[node].name,
					http: 'http://' + address,
					host: body.nodes[node].host
				}
				nodes.push(nodeObject)
			}
			callback(null, nodes)
		}
	})
}


// get cluster status
function clusterState(callback, nodeURL) {
	var url = nodeURL + '/_cluster/health'
	request({uri:url, json:true, proxy:null}, function(error, response, body) {
		if (error || response.statusCode != 200) {
			callback(error)
		} else {
			callback(null, body)
		}
	})
}


// wait until cluster is green
function waitForState(callback, nodeURL, retryTimes, key, value) {
	// wait between 1 second and 1 minute between retries
	var backoff = new Backoff({ min: 1000, max: 60000})
	async.retry(retryTimes, function(callback) {
		clusterState(function(error, state) {
			// retry on connection errors
			if (error) {
				console.log('cannot connect to', nodeURL)
				setTimeout(function() {
					callback(new Error('connection error to ' + nodeURL))
				}, backoff.duration())
			} else {
				// retry until state matches desired value
				if (state && state[key] === value) {
					callback(null, state)
				} else {
					console.log('Waiting on cluster state ' + key + ' to be ' + value + '. Currently: ' + state[key] || 'undefined')
					setTimeout(function() {
						callback(new Error(state[key]))
					}, backoff.duration())
				}				
			}

		}, nodeURL)
	}, function(error, result) {
		callback(error, result)
	})
}


// shutdown node
function shutdownNode(callback, nodeURL) {
	var url = nodeURL + '/_cluster/nodes/_local/_shutdown'
	request.post({uri:url, json:true, proxy:null}, function(error, response, body) {
		if (error || response.statusCode != 200)
			callback(error)
		else
			callback(null, body)
	})
}


// disable/enable cluster shard reallocation
function shardAllocation(callback, nodeURL, state) {
	var bodyPut = ''
	if (state === 'enable')
		bodyPut = {"transient" : {"cluster.routing.allocation.enable" : "all"}}
	else
		bodyPut = {"transient" : {"cluster.routing.allocation.enable" : "none"}}

	var url = nodeURL + '/_cluster/settings'

	request.put({uri:url, body:bodyPut, json:true, proxy:null}, function(error, response, body) {
		if (error || response.statusCode != 200 || body.acknowledged === false)
			callback(error)
		else
			callback(null, body)
	})
}


function startElasticSearch(callback, host, sleepSeconds) {
	var ssh = new sshclient.SSH({
    	hostname: host,
    	user: esUser,
    	port: 22
	})
	var esStartCmd = [
		'source .profile &&',
	    'sleep '+sleepSeconds+' &&',
	    esBinary,
	    '--config '+esConfig,
	    '-p '+esPID,
	    '-d'
	]
	var cmd = esStartCmd.join(' ')
	console.log(cmd)

	ssh.command(cmd, function(results) {
    	if (results.exitCode !== 0)
    		callback(results)
    	else
    		callback(null, results)
	})
}


async.series([
	function(callback) {
		console.log('Ensuring cluster is green...')
		waitForState(callback, clusterUrl, 3, 'status', 'green')
	},
	function(callback) {
		function assignGlobalNodes(error, data) {
			nodes = data
			callback(error, data)
		}
		console.log('Finding nodes in cluster...')
		clusterNodeURLs(assignGlobalNodes, clusterUrl)
	},
	function(callback) {
		function assignGlobalNodeCount(error, data) {
			nodeCount = data.number_of_nodes
			callback(error, data)
		}
		clusterState(assignGlobalNodeCount, clusterUrl)
	},
	function(callback) {
		console.log('nodes:', nodes)
		async.eachSeries(nodes, eachNode, function(error, results) {
			callback(error, results)
		})
	}
], function(error, results) {
	if (error) {
		console.log('Could not complete rolling restart due to the following error:')
		console.log(error)
		process.exit(1)
	}
	//console.log('RESULTS:')
	//console.dir(results, {depth: null, colors:true})
	console.log('')
	console.log('==== Cluster reboot is a success! ====')
})

function eachNode(node, callback) {
	async.series([
		function(callback) {
			console.log('')
			console.log('Working on node "' + node.name + '" running on ' + node.host)
			console.log('-------------------------------------------------------------')
			waitForState(callback, node.http, 10, 'status', 'green')
		},
		function(callback) {
			console.log('Disabling shard reallocation...')
			shardAllocation(callback, node.http, 'disable')
		},
		function(callback) {
			console.log('Shutting down node...')
			shutdownNode(callback, node.http)
		},
		function(callback) {
			console.log('Waiting 10s, then starting node...')
			startElasticSearch(callback, node.host, 10)
		},
		function(callback) {
			console.log('waiting for node to rejoin the cluster...')
			waitForState(callback, node.http, 10, 'number_of_nodes', nodeCount)
		},
		function(callback) {
			console.log('Enabling shard reallocation...')
			shardAllocation(callback, node.http, 'enable')
		},
		function(callback) {
			// waiting up to 60 minutes for node to initialize
			console.log('Waiting for node to initialize...')
			waitForState(callback, node.http, 60, 'status', 'green')
		}
	], function(error, results) {
		if (error) {
			console.log('node encountered error')
			console.log(error)
			process.exit(1)
		}
		console.log('--- Node restart a success ---')
		callback(error, results)
	})
}






