from flask_restful import Resource, reqparse
from performance_ozon_api import PerformanceOzonClient
from app import cache


class Report(Resource):
    @cache.cached(timeout=180)
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("client_id")
        parser.add_argument("client_secret")
        parser.add_argument("date_from")
        parser.add_argument("date_to")
        parser.add_argument("report_type")
        params = parser.parse_args()
        client = PerformanceOzonClient(params["client_id"], params["client_secret"])
        uuid = client.request_report(params["date_from"], params["date_to"], params["report_type"])
        link = client.get_link_to_requested_report(uuid)
        if link:
            return {"link": link}, 200
        else:
            return 404
