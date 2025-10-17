# -*- coding: utf-8 -*-
from odoo import http
from odoo.http import request, Response
import json

class ModelDetails(http.Controller):

    @http.route('/api/model_details', auth='public', type='http', methods=['GET'], csrf=False)
    def get_model(self, **kwargs):
        model = kwargs.get('model', '')

        if model == '':
            return Response(
                json.dumps({'status': 'error', 'message': 'Model name is required.'}),
                content_type='application/json;charset=utf-8',
                status=400
            )

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

        return Response(
            json.dumps(data),
            content_type='application/json;charset=utf-8',
            status=200
        )
