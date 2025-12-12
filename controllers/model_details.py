# -*- coding: utf-8 -*-
from odoo import http
from odoo.http import request, Response
import json

class ModelDetails(http.Controller):

    @http.route('/api/model_details', auth='public', type='http', methods=['GET'], csrf=False)
    def get_model(self, **kwargs):
        model = kwargs.get('model', '')
        data_like = kwargs.get('data_like', '')

        if model == '':
            return Response(
                json.dumps({'status': 'error', 'message': 'Model name is required.'}),
                content_type='application/json;charset=utf-8',
                status=400
            )

        if data_like == '':
            data_like = 'api_like'

        if data_like not in ['api_like', 'schema_like']:
            return Response(
                json.dumps({'status': 'error', 'message': 'Data like is invalid.'}),
                content_type='application/json;charset=utf-8',
                status=400  
        )

        if data_like == 'api_like':
            odoo_model = request.env['ir.model'].sudo().search(
                [
                    ('model', '=', model)
                ], 
                limit=1
            )

            data = {
                "name": odoo_model.model,
                "fields": {}
            }

            for field in odoo_model.field_id:
                data["fields"][field.name] = field.ttype

        elif data_like == 'schema_like':
            table_name = request.env[model]._table  # e.g. product_template

            query = """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = %s
                ORDER BY ordinal_position
            """

            request.env.cr.execute(query, (table_name,))
            columns = request.env.cr.dictfetchall()

            data = {
                "name": model,
                "fields": {
                    column["column_name"]: column["data_type"]
                    for column in columns
                }
            }


        return Response(
            json.dumps(data),
            content_type='application/json;charset=utf-8',
            status=200
        )
