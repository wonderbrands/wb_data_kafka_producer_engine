from io import BytesIO
from kafka import KafkaProducer
import logging
import traceback

_logger = logging.getLogger(__name__)


class KafkaProducerUtilities:
    def __init__(   self, 
                    odoo_env, 
                    message, 
                    topic, 
                    server, 
                    is_ssl=False, 
                    ssl_cert_attachment_id=None,
                    ssl_key_attachment_id=None,  
                    ssl_ca_attachment_id=None):
        self.env = odoo_env
        self.message = message
        self.topic = topic
        self.server = server
        self.is_ssl = is_ssl
        self.ssl_cert_attachment_id = ssl_cert_attachment_id
        self.ssl_key_attachment_id = ssl_key_attachment_id
        self.ssl_ca_attachment_id = ssl_ca_attachment_id
        
        
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
    
    def set_message(self, message):
        self.message = message
    
    def set_topic(self, topic):
        self.topic = topic

    def writeTmpCert(self):
        self.ssl_paths = {}
        if self.ssl_cert_attachment_id:
            self.ssl_paths['cert'] = '/tmp/ssl_cert.pem'
            with open(self.ssl_paths['cert'], 'w') as cert_file:
                cert_file.write(self.ssl_cert_attachment_id.decode('utf-8'))
        if self.ssl_key_attachment_id:
            self.ssl_paths['key'] = '/tmp/ssl_key.pem'
            with open(self.ssl_paths['key'], 'w') as key_file:
                key_file.write(self.ssl_key_attachment_id.decode('utf-8'))
        if self.ssl_ca_attachment_id:
            self.ssl_paths['ca'] = '/tmp/ssl_ca.pem'
            with open(self.ssl_paths['ca'], 'w') as ca_file:
                ca_file.write(self.ssl_ca_attachment_id.decode('utf-8'))

    def defineProducer(self):
        _logger.info("Defining Kafka Producer...")
        _logger.info(f"Server: {self.get_server()}")
        _logger.info(f"SSL: {self.get_is_ssl()}")

        config_producer = {}
        config_producer['bootstrap_servers'] = self.get_server()
        config_producer['security_protocol'] = 'SSL' if self.get_is_ssl() else 'PLAINTEXT'
        if self.get_is_ssl():
            self.writeTmpCert()
            config_producer['ssl_cafile'] = self.ssl_paths['ca']
            config_producer['ssl_certfile'] = self.ssl_paths['cert']
            config_producer['ssl_keyfile'] = self.ssl_paths['key']

        self.producer = KafkaProducer(**config_producer)


    def sendMessage(self):
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
            self.producer.send(self.get_topic(), self.get_message().encode('utf-8'))
        except Exception as e:
            _logger.error(traceback.format_exc())
            _logger.error(f"Error sending message to Kafka: {e}")

    def kill(self):
        producer.flush()
        producer.close()
        if os.path.exists('/tmp/global-bundle.pem'):
            os.remove('/tmp/global-bundle.pem')
