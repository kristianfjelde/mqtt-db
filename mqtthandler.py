import paho.mqtt.client as mqtt
from threading import Thread
import json
import _mysql
import _mysql_exceptions


BROKER = "test.mosquitto.org"
PORT = 1883

DBHOST = "mysql.stud.ntnu.no"
DBUSER = "kristiap_test"
DBPASS = "ttm411515"
DBNAME = "kristiap_ttm4115"

# TODO: Achieve db connection  [X]
# TODO: Solve "_mysql.connection' object has no attribute 'cursor"

class mqttHandler:

    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code {}".format(rc))


    def on_message(self, client, userdata, msg):
        print("Received message from topic {}".format(msg.topic))
        if msg.topic == "ttm4115/15/server/fetchs":
            dbHandler.fetchhisotry(msg.payload)
        elif msg.topic == "ttm4115/15/server/routeplanner":
            dbHandler.fetchfull(msg.payload)
        elif msg.topic == "ttm4115/15/server/update":
            dbHandler.updatestatus(msg.payload)
        elif msg.topic == "ttm4115/15/server/register":
            dbHandler.registerUser(msg.payload)
        else:
            print("Publish to topic not handled: {}".format(msg.topic))

    def send_message(self, topic, payload):
        self.client.publish(topic, payload)

    def get_client(self):
        return self.client

    def __init__(self):
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        self.client.connect(BROKER, PORT)
        self.client.subscribe("ttm4115/15/server/#")
        print("Successfully connected to Broker")

        #thread = Thread(target=self.client.loop_forever())
        #thread.start()
        #thread.join()


class dbHandler:

    def fetchhistory(self, payload):
        #SELECT b.amount, b.time, a.address
        #FROM users a, bins b
        #WHERE a.bin_id = b.bin_id
        #ORDER BY time ASC;

        data = json.loads(payload)
        try:
            for element in data['adresses']:
                self.cur.execute("SELECT * FROM users")
                self.cur.fetchall()

        except UnboundLocalError:
            print("'adresses' not found in payload")



    def register(self, payload):
        data = json.loads(payload)

        for element in data['adresses']:
            self.cur.execute("SELECT * FROM adresses")
            receive = self.cur.fetchall()

        print("'adresses' not found in payload")

        json_data = json.dumps(recv)
        mqttHandler.send_message("ttm4115/15/workstation/server", json_data)


    def updatestatus(self, payload):
        data = json.loads(payload)
        try:
            for element in data['adresses']:
                self.cur.execute("INSERT")
                self.cur.fetchall()

        except UnboundLocalError:
            print("Unable to execute")


    def __init__(self):

        try:
            self.db = _mysql.connect(DBHOST, DBUSER, DBPASS, DBNAME)
            print("Connected successfully")
            self.db.close()
        except (_mysql_exceptions.OperationalError, NameError) as err:
            print("Error in db connection: {0}".format(err))
            exit(1)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()



if __name__ == '__main__':
    dbHandler()

    mqttHandler = mqttHandler()
    client = mqttHandler.get_client()
    thread = Thread(target=client.loop_forever())
    thread.start()


    client.publish("ttm4115/15/server/ls", "jfhjas", 1)
    print("Sent to ttm4115/15/server/ls")

    thread.join()
    print("Thread finished. Exiting...")

