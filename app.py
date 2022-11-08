from flask import Flask
from flask_restful import Api
from flask_caching import Cache
from reports_api import Report

app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE': 'simple'})
api = Api(app)
api.add_resource(Report, "/reports/")

if __name__ == '__main__':
    app.run(debug=True)
