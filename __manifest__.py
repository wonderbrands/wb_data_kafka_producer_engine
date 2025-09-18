# -*- coding: utf-8 -*-
{
    'name': "Kafka producer engine",

    'summary': "Lists a group of models and fields that whenever a record is created or updated, it will be sent to Kafka",

    'description': """
    Defines a group of models and fields that whenever a record is created or updated, 
    it will be sent to Kafka, defined as a producer on a topic 
    """,

    'author': "Wonder Brands",
    'website': "https://wonderbrands.odoo.com/",

    'category': 'Technical',
    'version': '15.0',

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
        'views/views.xml',
        'actions/actions.xml',
        'menu/root.xml',
        'menu/submenu.xml',
    ],
}

