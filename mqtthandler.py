import paho.mqtt.client as mqtt
from threading import Thread
import json
import _mysql
import _mysql_exceptions

BROKER = "188.166.100.22"
PORT = 1883

DBHOST = "localhost"
DBUSER = "ttm4115"
DBPASS = "ttm411515"
DBNAME = "ttm4115"

# TODO: Achieve db connection  [X]
# TODO: Solve "_mysql.connection' object has no attribute 'cursor"

class mqttHandler:

    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code {}".format(rc))


    def on_message(self, client, userdata, msg):
        print("Received message from topic {}".format(msg.topic))
        if msg.topic == "ttm4115/15/server/fetch":
            res = dbHandler.fetchhistory(self.d, msg.payload)
            self.client.publish("ttm4115/15/userdevice", res)

        elif msg.topic == "ttm4115/15/server/routeplanner":
            res = dbHandler.routeplanner(self.d)
            self.client.publish("ttm4115/15/fulladdresses", res)

        elif msg.topic == "ttm4115/15/server/update":
             res = dbHandler.updatestatus(self.d, msg.payload)
             if res == "err":
                 self.client.publish("ttm4115/15/hardware/", "Invalid format")

        elif msg.topic == "ttm4115/15/server/register":
            res = dbHandler.register(self.d, msg.payload)
            if res == "err":
                self.client.publish("ttm4115/15/workstation", "Invalid format")

        elif msg.topic == "ttm4115/15/server/fetchaddresses":
            res = dbHandler.fetchaddresses(self.d)
            self.client.publish("ttm4115/15/addresses", res)

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



class dbHandler:

    def routeplanner(self):
        formatted_string = "SELECT bin_id, address FROM users;"
        res = self.doquery(formatted_string)

        temp = res.fetch_row(0,1)
        list = []

        for element in temp:
            temp_bin = element['bin_id'].decode('utf-8')
            res = self.doquery("SELECT amount FROM bins WHERE bin_id = '{0}' ORDER BY time DESC".format(temp_bin))
            temp_query = res.fetch_row(1,1)

            if len(temp_query) is not 0:
                if 'amount' in temp_query[0]:
                    if int(temp_query[0]['amount']) >= 70:
                        list.append(element['address'].decode('utf-8'))

        end = json.dumps(list)
        return end



    def fetchaddresses(self):
        formatted_string = "SELECT address FROM users;"
        res = self.doquery(formatted_string)

        list = []
        temp = res.fetch_row(0,1)
        for element in temp:
            list.append(element['address'].decode('utf-8'))

        dict = {'addresses' : list}
        return json.dumps(dict)


    def addresshistory(self, address):
        formatted_string = "SELECT * FROM users WHERE address = '{0}' ORDER BY time ASC;".format(address)
        res = self.doquery(formatted_string)

        if res is not None:
            bin_id = res.fetch_row(0,1)[0]['bin_id'].decode('utf-8')
        else:
            return "err"

        formatted_string = "SELECT bin_id, amount, time FROM bins WHERE bin_id = '{0}';".format(bin_id)
        res = self.doquery(formatted_string)

        iter_list = res.fetch_row(0,1)
        history_list = []
        for element in iter_list:
            history_list.append([element['time'], element['amount']])

        return history_list


    def fetchhistory(self, payload):
        try:
            addresses = json.loads(payload)
        except TypeError as err:
            print("TypeError has occured")
            print(err)
            addresses = json.loads(payload.decode('utf-8'))

        res = {}
        list = {}

        for element in addresses:
            list.update({element : self.addresshistory(element)})

        res.update({"addresses" : list})

        print(json.dumps(res))
        return json.dumps(res)



    def register(self, payload):
        try:
            data = json.loads(payload)
        except json.decoder.JSONDecodeError as err:
            print(err)
            return "err"

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
            return self.res

        self.db.close()


    def __init__(self):
        try:
            self.db = _mysql.connect(DBHOST, DBUSER, DBPASS, DBNAME)
            self.db.close()

        except (_mysql_exceptions.OperationalError, NameError) as err:
            print("Error in db connection: {0}".format(err))
            exit(1)

        self.routeplanner()


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()



if __name__ == '__main__':

    mqttHandler = mqttHandler()
    client = mqttHandler.get_client()
    thread = Thread(target=client.loop_forever())
    thread.start()

    thread.join()
    print("Thread finished. Exiting...")

