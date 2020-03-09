import os

import jsonpickle
from flask import Flask, request, Response, render_template, flash, redirect
from werkzeug.utils import secure_filename

from src.utilities import sken_logger, db, sken_singleton,constants
from src.services import dimension_engine
from src.services import facet_service

logger = sken_logger.get_logger("main")

sken_singleton.Singletons.get_instance()
db.DBUtils.get_instance()
tmp_pro_id = None  # used to catch and reset the catch if new product request is made
request_count = 0

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route("/upload_file", methods=["POST", "GET"])
def upload_csv():
    global tmp_pro_id, request_count

    if request.method == "POST":
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
    file = request.files['file']
    threshold = request.form.get("threshold")
    org_id = request.form.get("organization")
    product_id = request.form.get("product_id")
    if org_id:
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
    input_filename = secure_filename(file.filename)
    input_file_path = os.path.join(app.config[constants.fetch_constant("UPLOAD_FOLDER")], input_filename)
    if os.path.exists(input_file_path):
        logger.info("File path {} already exists so removing this file".format(input_file_path))
        os.remove(input_file_path)
    logger.info("Making new file {}".format(input_file_path))
    file.save(input_file_path)

    if request_count == 0:
        logger.info("This is the first request for organization={} and product={}".format(org_id, product_id))
        tmp_pro_id = product_id
        request_count += 1
        resp = Response(jsonpickle.encode(dimension_engine.wrapper_method(input_file_path, org_id, product_id, threshold)),
                        mimetype='application/json')
        resp.headers['Access-Control-Allow-Origin'] = '*'

    elif request != 0 and tmp_pro_id != product_id:
        logger.info(
            "First request for organization={} and product={} clearing the cache_facets for old_product={}".format(
                org_id, product_id, tmp_pro_id))
        dimension_engine.refresh_cached_dims(org_id, product_id)
        request_count = 1
        tmp_pro_id = product_id
        resp = Response(jsonpickle.encode(dimension_engine.wrapper_method(input_file_path, org_id, threshold)),
                        mimetype='application/json')
        resp.headers['Access-Control-Allow-Origin'] = '*'

    else:
        request_count += 1
        logger.info("This is {} request for  organization={} and product={}".format(request_count, org_id, tmp_pro_id))
        resp = Response(jsonpickle.encode(dimension_engine.wrapper_method(input_file_path, org_id, threshold)),
                        mimetype='application/json')
        resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp


@app.route("/generate_paraphrase", methods=["POST", "GET"])
def generate_paraphrase():
    org_id = request.args.get("org_id")
    product_id = request.args.get("prod_id")
    if facet_service.make_generated_facet_siganls(org_id, product_id):
        text = "Made generated facet signals for org_id={} and product_id={}".format(org_id, product_id)
    else:
        text = "Could not find any facet signals for org_id={} and product_id={}".format(org_id, product_id)
    resp = Response(text,
                    mimetype='application/text')
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp


@app.route("/make_facet_signals", methods=["POST", "GET"])
def make_facet_signals():
    if request.method == "POST":
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
    file = request.files['file']
    org_id = request.form.get("organization")
    prod_id = request.form.get("product_id")
    if org_id:
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
    input_filename = secure_filename(file.filename)
    input_file_path = os.path.join(app.config['UPLOAD_FOLDER'], input_filename)
    if os.path.exists(input_file_path):
        logger.info("File path {} already exists so removing this file".format(input_file_path))
        os.remove(input_file_path)
    logger.info("Making new file {}".format(input_file_path))
    file.save(input_file_path)

    resp = Response(facet_service.make_facet_signal_entries(input_file_path),
                    mimetype='application/text')
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
