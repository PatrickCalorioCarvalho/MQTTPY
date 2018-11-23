#create table IF NOT EXISTS MQTTBD(ID int not null auto_increment primary key,DataRecebimento datetime not null,Topico varchar(100) not null,Mensagen varchar(100) not null,Retida boolean not null);
#python -m pip install paho-mqtt
import paho.mqtt.client as mqtt
#python -m pip install mysql-connector
import mysql.connector
import datetime

mydb = mysql.connector.connect(
  host="10.0.1.150",
  user="root",
  passwd="*Smart2014",
  database='sys'
)

def on_connect(client, userdata, flags, rc):
    client.subscribe("#")

def on_message(client, userdata, msg):
    if msg.topic == "/sensor/temperatura":

       print("Temperatura : "+msg.payload.decode()+"ºC")
       mycursor = mydb.cursor()
       sql = "INSERT INTO MQTTBD(DataRecebimento,Topico,Mensagen,Retida) VALUES(%s, %s, %s, %s);"
       val = (str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")), msg.topic,msg.payload.decode(),str(msg.retain))
       mycursor.execute(sql, val)
       mydb.commit()
       print("registro inserido com sucesso - " +str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))

    if msg.topic == "/sensor/umidade":
       print("Umidade : "+msg.payload.decode()+"%")
       mycursor = mydb.cursor()
       sql = "INSERT INTO MQTTBD(DataRecebimento,Topico,Mensagen,Retida) VALUES(%s, %s, %s, %s);"
       val = (str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")), msg.topic,msg.payload.decode(),str(msg.retain))
       mycursor.execute(sql, val)
       mydb.commit()
       print("registro inserido com sucesso - "+str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.username_pw_set("administrator", password="*Smart2014")
client.connect("10.0.1.150", 1883, 60)
client.loop_forever()
