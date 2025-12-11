from ..ports import MessageBroker
from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

class MSKTokenProvider(AbstractTokenProvider):
    def __init__(self, region):
        self.region = region
    
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
        return token

def create_consumer(bootstrap_servers, topic_name, region='us-east-1', offset_reset='earliest'):
    tp = MSKTokenProvider(region)
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=tp,
        client_id=f"{socket.gethostname()}-consumer",
        group_id=None,
        auto_offset_reset=,
        enable_auto_commit=False,
    )
    return consumer

class KafkaMessageBroker(MessageBroker):
    def authenticate(self, host, port, user, password, database_name, any_other_credential=None):
        try:
            consumer = create_consumer(
                                        any_other_credential["bootstrap_servers"], 
                                        any_other_credential["topic_name"]
                                    )
            return consumer
        except Exception as e:
            raise Exception(e)

    def consume(self, consumer, database):
        try:
            while True:
                message_batch = consumer.poll(timeout_ms=5000)
                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            result = None
                            while not result:
                                print(f"Consuming message, trying to input: {message.value}")
                                result = database.handle_input(message.value)
                    consumer.commit()
    except KeyboardInterrupt:
        print(f"\n👋 Goodbye!")
    finally:
        consumer.close()
        print("Consumer closed cleanly.")

            
