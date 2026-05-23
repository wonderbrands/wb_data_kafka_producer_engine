# -*- coding: utf-8 -*-
{
    'name': "Kafka Producer Engine",

    'summary': "Lists a group of models and fields that whenever a record is created or updated, it will be sent to Kafka",

    'description': """
    Defines a group of models and fields that whenever a record is created or updated, 
    it will be sent to Kafka, defined as a producer on a topic 
    """,

    'author': "Wonder Brands",
    'website': "https://wonderbrands.odoo.com/",

    'category': 'Technical',
    'version': '18.0',

    'depends': ['base'],

    'external_dependencies': {
        'python': [
            'aiokafka',
            'async-timeout',
            'aws-msk-iam-sasl-signer-python',
            'boto3',
            'botocore',
            'click',
            'confluent-kafka',
            'jmespath',
            'kafka-python',
            'packaging',
            'python-dateutil',
            's3transfer',
            'six',
            'typing_extensions',
            'urllib3',
        ],
    },


    'data': [
        'groups/kafka_admin.xml',
        'security/ir.model.access.csv',
        'views/res_config_settings_views.xml',
        'views/views.xml',
        'actions/actions.xml',
        'menu/root.xml',
        'menu/submenu.xml',
    ],
    'assets': {
        'web.assets_backend': [
            'wb_data_kafka_producer_engine/static/src/components/dump_progress_bar/dump_progress_bar.js',
            'wb_data_kafka_producer_engine/static/src/components/dump_progress_bar/dump_progress_bar.xml',
            'wb_data_kafka_producer_engine/static/src/components/kafka_manager/kafka_manager.js',
            'wb_data_kafka_producer_engine/static/src/components/kafka_manager/kafka_manager.xml',
        ],
    },
}

