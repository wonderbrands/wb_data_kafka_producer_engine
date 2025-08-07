import asyncio
from io import BytesIO
from kafka import KafkaProducer

class KafkaProducerUtilities:
    def __init__(self, odoo_env, message, topic, server, is_ssl=False, cert=None):
        self.env = odoo_env
        self.message = message
        self.topic = topic
        self.server = server
        self.is_ssl = is_ssl
        self.cert = cert
        self.producer = self.defineProducer()
        
    def get_message(self):
        return self.message
    
    def get_env(self):
        return self.env

    def get_topic(self):
        return self.topic

    def get_server(self):
        return self.server

    def get_is_ssl(self):
        return self.is_ssl

    def get_cert(self):
        return self.cert
    
    def set_message(self, message):
        self.message = message

    def writeTmpCert(self):
        with open('/tmp/global-bundle.pem', 'w') as f:
            f.write(self.get_cert())

    def defineProducer():
        config_producer = {}
        config_producer['bootstrap.servers'] = self.get_server()
        config_producer['security.protocol'] = 'SSL' if self.get_is_ssl() else 'PLAINTEXT'
        if self.get_is_ssl():
            self.writeTmpCert()
            config_producer['ssl.ca.location'] = '/tmp/global-bundle.pem' 

        producer = KafkaProducer(**config_producer)
        return producer


    async def sendMessage(self):
        #SEND NUDES 
        """
               ...gNMMM@@MMMNa+..
           ..M@M#"=!........?7TWMMN&,
        ..MMY=...`.....`...```....?YMMa.
      .JM#^..``.```````````````....`.JW@N.
     .M#^.``.`.```````````````.`..````.,WMh.
   .MM5.J.J..```.````````..`.JJ.,,`...``.JMN.
  .MM.JY^` .MMa .```````.`.M"=  .dMN,`.`.`.MM,
 .@M.M$    @@MMN.``````.`dF     J@@MMp````..MM,
 M@^JF     ?WH"Jb .`````JF       "H"^M,..```.@N
JMF dF         JN `````.@F           gF `.`.`J@F
J@'`Jh.........JF .````.4b...........MF````.`.@b
M@ `.!!!!!!!!!!?````````.!!!??????????.`````` @N
M@ `.......`.`.``.`.....`````.`....`...````.. @M
d@,``.NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNr````.``.@F
J@b ```JNWHHHHHHHHHHHHHHHHHHHHHHHHHHd@ ``````J@F
 MM.``` @KHHHHHHHHHHHHHHHHHHHHHHHHHHW@.````..@M
 .@N ``.JMKHHHHHHHHHHHHHHHHHHHHHHHHHM@ ```.`dM3
  J@N.```?MKHHHHHHHHHHHNHYY9HHHHHHHHMF.``..d@$
   .MN,``.JMNHHHHHHHNY!.`````..4NHNM5..``.MM^
     4Mh,`..TMNHHHH@ ...```...`..MD..``.JMY`
      .W@N,``.?HNN# ...`..``J.M#^.`.`.J@#^
        .TMMa,`..`T"MMQQHMH"T!..``..@MY!
           `"MMNg...````````...+MMM"^
               `""WMMM@@@MM@M#""!"""
        try:
            await producer.send(self.get_topic(), self.get_message())
        except Exception as e:
            print(e)

    def kill():
        producer.flush()
        producer.close()
        if os.path.exists('/tmp/global-bundle.pem'):
            os.remove('/tmp/global-bundle.pem')
