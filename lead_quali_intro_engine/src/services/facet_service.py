import os

import pandas as pd
from google.cloud import translate
from src.utilities import db, constants, sken_logger

client = translate.TranslationServiceClient()
logger = sken_logger.get_logger("facet_signal")


def make_facet_signal_entries(file_path, org_id, prod_id):
    df = pd.read_excel(file_path)
    for facet_signal in range(len(df)):
        sql = """insert into facet_signal (value, facet_id, org_id, product_id) values(%s, (select 
        id from facet where name_ = %s), %s, %s) """
        db.DBUtils.get_instance().execute_query(sql, (df.text[facet_signal], df.facet[facet_signal], org_id, prod_id),
                                                is_write=True, is_return=False)


def praphrase_sentences(text, depth=int(constants.fetch_constant("language_depth")),
                        project_id=constants.fetch_constant("google_project_id")):
    parent = client.location_path(project_id, "global")
    x = client.get_supported_languages(parent)
    target_laguages = [item.language_code for item in x.languages[:depth]]
    translated_text = []
    for language in target_laguages:
        response = client.translate_text(
            parent=parent,
            contents=[text],
            mime_type='text/plain',  # mime types: text/plain, text/html
            source_language_code='en-IN',
            target_language_code=language)
        for translation in response.translations:
            translated_text.append(translation.translated_text)
    result = []
    for lg, sentence in zip(target_laguages, translated_text):
        response = client.translate_text(
            parent=parent,
            contents=[sentence],
            mime_type='text/plain',  # mime types: text/plain, text/html
            source_language_code=str(lg),
            target_language_code="en")
        for translation in response.translations:
            result.append(translation.translated_text)
    return result


def make_generated_facet_siganls(org_id, product_id):
    sql = "select 	dimension.id as dimid, 	dimension.name_ as dimname, 	facet.id as facet_id, 	facet.name_ as " \
          "facet_name, 	facet_signal.id as fsid, 	facet_signal.value as fsval, 	facet_signal.org_id, 	" \
          "facet_signal.product_id from 	dimension left join facet on 	facet.dim_id = dimension.id left join " \
          "facet_signal on 	facet_signal.facet_id = facet.id left join generated_facet_signals on 	" \
          "generated_facet_signals.facet_signal_id = facet_signal.id where facet_signal.org_id = %s and " \
          "facet_signal.product_id = %s group by 	dimension.id, 	facet.id, 	facet_signal.id "
    rows, col_names = db.DBUtils.get_instance().execute_query(sql, (org_id, product_id), is_write=False, is_return=True)
    if len(rows) != 0:
        for row in rows:
            logger.info("Making paraphrase sentences for text={} ".format(row[col_names.index("fsval")]))
            sentences = praphrase_sentences(row[col_names.index("fsval")])
            sentences.append(row[col_names.index("fsval")])
            for sentence in list(set(sentences)):
                sql = "INSERT INTO public.generated_facet_signals (value, facet_signal_id) VALUES(%s, %s)"
                db.DBUtils.get_instance().execute_query(sql, (sentence, row[col_names.index("fsid")]), is_write=True,
                                                        is_return=False)
            logger.info("Generated paraphrased sentences for facet_signal_id={}".format(row[col_names.index("fsid")]))
        logger.info("done with making paraphrases for org_id={} and product_id={}".format(org_id, product_id))
        return True
    else:
        logger.info("No facet signals found for org_id={} and product_id={}".format(org_id, product_id))
        return False


if __name__ == "__main__":
    make_generated_facet_siganls(1, 150)
