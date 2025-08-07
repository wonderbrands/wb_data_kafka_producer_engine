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
        'python': ['kafka-python'],
    },

    'data': [
        'views/config_producer_form.xml',
        'views/config_producer_list.xml',
        'views/failed_sends_list.xml',
        'views/followed_fields_form.xml',
        'views/followed_fields_list.xml',

        'actions/config_producer.xml',
        'actions/failed_sends.xml',
        'actions/followed_fields.xml',
        
        'menu/root.xml',
        'menu/submenu.xml',
    ],
}

