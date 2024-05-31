$(document).ready(function() {

    function webSocketInvoke() {

        if ("WebSocket" in window) {
            console.log("WebSocket is supported by your Browser!");
            var ws = new WebSocket("ws://localhost:8080/", "echo-protocol");

            ws.onopen = function() {
                console.log("Connection created");
            };

            ws.onmessage = function(evt) {
                var received_msg = evt.data;
		var li = document.createElement("li");
		
		var dataDisplay = document.getElementById("data-display");
		li.textContent = evt.data;
		dataDisplay.appendChild(li);
                
                console.log(received_msg);
            };

            ws.onclose = function() {
                console.log("Connection closed");
            };
        } else {
            alert("WebSocket NOT supported by your Browser!");
        }
    }
    webSocketInvoke();
});
