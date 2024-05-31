console.log("Starting service ...");
var WebSocketServer = require('websocket').server;
var http = require('http');

var kafka = require('kafka-node');
var Consumer = kafka.Consumer,
    client = new kafka.KafkaClient("localhost:9092"),
    consumer = new Consumer(
        client, [{
            topic: 'Test',
            partition: 0
        }], {
            autoCommit: false
        });

var server = http.createServer(function(request, response) {
    console.log('Request recieved: '+request.url);
    response.writeHead(404);
    response.end();
});
server.listen(8080, function() {
    console.log('Listening on port: 8080');
});

webSocketServer = new WebSocketServer({
    httpServer: server,
    autoAcceptConnections: false
});

function iSOriginAllowed(origin) {
    return true;
}

webSocketServer.on('request', function(request) {
    if (!iSOriginAllowed(request.origin)) {
        request.reject();
        console.log('Connection from: '+request.origin + 'rejected.');
        return;
    }

    var connection = request.accept('echo-protocol', request.origin);
    console.log('Connection accepted: '+request.origin);
    connection.on('message', function(message) {
        if (message.type === 'utf8') {
            console.log('Received Message: '+message.utf8Data);
        }
    });
    consumer.on('message', function(message) {
        console.log(message);
        connection.sendUTF(message.value);
    });
    connection.on('close', function(reasonCode, description) {
        console.log('Connection' + connection.remoteAddress + 'disconnected.');
    });
});
