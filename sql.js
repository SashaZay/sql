var mqtt = require('mqtt');
var Topic = 'msh/2/json/R38/!ea245858';
var Broker_URL = 'mqtt://10.0.7.100';
var Database_URL = '10.0.7.100';

var options = {
	clientId: 'MyMQTT',
	port: 1883,
	username: 'home',
	password: '5555',	
	keepalive: 60
};

var client = mqtt.connect(Broker_URL, options);
client.on('connect', mqtt_connect);
client.on('reconnect', mqtt_reconnect);
client.on('message', mqtt_messsageReceived);
client.on('close', mqtt_close);

function mqtt_connect(){
    console.log("Connecting MQTT");
    client.subscribe(Topic, mqtt_subscribe);
};

function mqtt_subscribe(err, granted){
    console.log("Subscribed to " + Topic);
    if (err) {console.log(err);}
};

function mqtt_reconnect(err){
    console.log("Reconnect MQTT");
    if (err) {console.log(err);}
	client  = mqtt.connect(Broker_URL, options);
};

function after_publish(){
	
};

function mqtt_messsageReceived(packet, message){
	var message_str = message.toString();
	message_str = message_str.replace(/\n$/, '');
	console.log("message", message_str);
	if(message_str.length == 0){
		console.log("Invalid payload");
		} else {
			if(message_str.includes('position')){
				insert_message(message_str, packet);
			}
	}
};

function mqtt_close(){
	
};

var mysql = require('mysql');
var connection = mysql.createConnection({
	host: Database_URL,
	user: "root",
	password: "777",
	database: "map"
});

connection.connect(function(err){
	if(err) throw err;
});

function insert_message(message_str, packet){
	var message_arr = extract_string(message_str);
	var uid = message_arr[2].split(',').shift();
	var topic = 0;
	var latitude = message_arr[10].split(',').shift();
	var longitude = message_arr[11].split(',').shift();
	
	connection.query("SELECT `nomer` FROM `location` WHERE `id`= ?", [uid], function(error, res){
		if(error) throw error;
		var nomer = res[0]['nomer'];
		
		var sql = "REPLACE INTO `location` (??,??,??,??,??) VALUES (?,?,?,?,?)";
		var params = ['id', 'topic', 'latitude', 'longitude', 'nomer', uid, topic, latitude, longitude, nomer];
		sql = mysql.format(sql, params);
		
		connection.query(sql, function (error, results){
			if(error) throw error;
			console.log("Message added: " + message_str);
		});
	});
};

function extract_string(message_str){
	var message_arr = message_str.split(":");	
	return message_arr;
};	

var delimiter = ",";
function countInstances(message_str){
	var substrings = message_str.split(delimiter);
	return substrings.length - 1;
};
