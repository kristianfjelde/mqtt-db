import paho.mqtt.client as mqtt
from threading import Thread
import json
import _mysql
import _mysql_exceptions

BROKER = "188.166.100.22"
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
        if msg.topic == "ttm4115/15/server/fetch":
            dbHandler.fetchhistory(self.d, msg.payload)

        elif msg.topic == "ttm4115/15/server/routeplanner":
            dbHandler.routeplanner(self.d, msg.payload)

        elif msg.topic == "ttm4115/15/server/update":
             res = dbHandler.updatestatus(self.d, msg.payload)
             if res == "err":
                 self.client.publish("ttm4115/15/hardware/", "Invalid format")

        elif msg.topic == "ttm4115/15/server/register":
            res = dbHandler.register(self.d, msg.payload)
            if res == "err":
                self.client.publish("ttm4115/15/workstation", "Invalid format")

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

        self.d = dbHandler()

        #thread = Thread(target=self.client.loop_forever())
        #thread.start()
        #thread.join()


class dbHandler:

    def fetchhistory(self, payload):
        #SELECT b.amount, b.time
        #FROM users a, bins b
        #WHERE a.bin_id = b.bin_id
        #ORDER BY time ASC;

        #TODO: Returnerer amount og timestamp
        #Todo: Må klare å spørre etter for gitt addresse

        #SELECT * FROM users WHERE address = "Oljeveien 1"
        # WHERE address = {0}".format("Oljeveien 1"))

        data = json.loads(payload.decode('utf-8'))
        try:
            for element in data['adresses']:
                self.db.query("SELECT * FROM users")
                self.db.fetchall()

        except UnboundLocalError:
            print("'adresses' not found in payload")

    def routeplanner(self, payload):
        data = json.loads(payload.decode('utf-8'))
        print("Routeplanner activated")




    def register(self, payload):
        data = json.loads(payload.decode('utf-8'))
        if len(data) == 2 and data[0][:3] == "bin":
            formatted_string = """
            INSERT INTO users (bin_id, address, time)
            VALUES ('{0}', '{1}', NOW());""".format(data[0], data[1])
            print(formatted_string)
            self.doquery(formatted_string)
        else:
            return "err"


    def updatestatus(self, payload):
        data = json.loads(payload.decode('utf-8'))
        if len(data) == 2 and data[0][:3] == "bin":
            formatted_string = """
            INSERT INTO bins (bin_id, amount, time) 
            VALUES ('{0}', {1}, NOW());""".format(data[0], data[1])

            res = self.doquery(formatted_string)
            return "Success"
        else:
            return "err"


    def doquery(self,msg):
        self.db = _mysql.connect(DBHOST, DBUSER, DBPASS, DBNAME)
        try:
            self.db.query(msg)
            self.res = self.db.store_result()

        except (_mysql_exceptions.MySQLError, _mysql_exceptions.DataError) as err:
             print("Failed to execute due to {}".format(err))

        if self.res is not None:
            print("Results: ")
            print(self.res.fetch_row())
            return self.res

        self.db.close()


    def __init__(self):
        try:
            self.db = _mysql.connect(DBHOST, DBUSER, DBPASS, DBNAME)
            self.db.close()

        except (_mysql_exceptions.OperationalError, NameError) as err:
            print("Error in db connection: {0}".format(err))
            exit(1)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()



if __name__ == '__main__':

    mqttHandler = mqttHandler()
    client = mqttHandler.get_client()
    thread = Thread(target=client.loop_forever())
    thread.start()

    thread.join()
    print("Thread finished. Exiting...")

