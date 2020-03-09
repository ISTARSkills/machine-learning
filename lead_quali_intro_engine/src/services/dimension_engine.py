import time

from src.utilities import sken_singleton, sken_logger, constants,sken_exceptions
import os
import pandas
from src.utilities.db import DBUtils
from src.utilities.objects import FacetSignal, Facet
from src.services import snippet_service

from concurrent.futures import ThreadPoolExecutor

logger = sken_logger.get_logger("dimension_engine")


def refresh_cached_dims(org_id, prod_id):
    """
    This method refreshes the cached_dimensions singleton ,the method clears the cached dimensions whenever a new
    product request is made
    :return:
    """

    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(sken_singleton.Singletons.get_instance().get_cached_lq_dims().clear())
        executor.submit(sken_singleton.Singletons.get_instance().get_cached_lq_dims().clear())
    logger.info("Cleared cached dimensions for org_id ={} and product_id={}".format(org_id, prod_id))


def make_cached_dimensions(org_id, prod_id):
    """
    This method caches the facet signals for the particular product and organization
    :param org_id:
    :param prod_id:
    :return:
    """
    if len(sken_singleton.Singletons.get_instance().get_cached_lq_dims()) == 0 or len(
            sken_singleton.Singletons.get_instance().get_cached_intro_dims()) == 0:
        logger.info("Creating cached_dimensions for organization={} and product={}".format(org_id, prod_id))
        sql = "select dimension.id as dimid,dimension.name_ as dimname,facet.id as facet_id,facet.name_ as " \
              "facet_name,facet_signal.id as fsid,facet_signal.value as fsval,generated_facet_signals.id as gsid," \
              "generated_facet_signals.value as gs_value,facet_signal.org_id,facet_signal.product_id from dimension " \
              "left join facet on facet.dim_id = dimension.id left join facet_signal on facet_signal.facet_id = " \
              "facet.id left join generated_facet_signals on 	generated_facet_signals.facet_signal_id = " \
              "facet_signal.id where facet_signal.org_id=%s and facet_signal.product_id=%s group by dimension.id," \
              "facet.id,facet_signal.id,generated_facet_signals.id "

        rows, col_names = DBUtils.get_instance().execute_query(sql, (org_id, prod_id), is_write=False, is_return=True)
        kvp_id = as_id = a_id = b_id = i_id = n_id = None
        if len(rows) != 0:
            start = time.time()
            logger.info("Making cache facet signals for organization= {} and product={}".format(org_id, prod_id))
            kvp_facet_signals = []
            as_facet_signals = []
            authority_facet_singals = []
            budget_facte_singals = []
            interest_face_signals = []
            need_facet_singals = []
            for row in rows:
                if str(row[col_names.index("dimname")]).lower() == "introduction":
                    if str(row[col_names.index("facet_name")]).lower() == "key value proposition":
                        kvp_id = row[col_names.index("fsid")]
                        if row[col_names.index("gs_value")] is not None:
                            kvp_facet_signals.append(
                                FacetSignal(row[col_names.index("gsid")], row[col_names.index("gs_value")],
                                            row[col_names.index("fsid")],
                                            embedding=sken_singleton.Singletons.get_instance().perform_embeddings(
                                                row[col_names.index("gs_value")]),
                                            embedding_method=constants.fetch_constant("encoding_method")))
                        else:
                            kvp_facet_signals.append(
                                FacetSignal(row[col_names.index("gsid")], row[col_names.index("gs_value")],
                                            row[col_names.index("fsid")],
                                            embedding=None,
                                            embedding_method=None))
                    else:
                        as_id = row[col_names.index("fsid")]
                        if row[col_names.index("gs_value")] is not None:
                            as_facet_signals.append(
                                FacetSignal(row[col_names.index("gsid")], row[col_names.index("gs_value")],
                                            row[col_names.index("fsid")],
                                            embedding=sken_singleton.Singletons.get_instance().perform_embeddings(
                                                row[col_names.index("gs_value")]),
                                            embedding_method=constants.fetch_constant("encoding_method")))
                        else:
                            as_facet_signals.append(
                                FacetSignal(row[col_names.index("gsid")], row[col_names.index("gs_value")],
                                            row[col_names.index("fsid")],
                                            embedding=None,
                                            embedding_method=None))
                else:
                    if str(row[col_names.index("facet_name")]).lower() == "authority":
                        a_id = row[col_names.index("fsid")]
                        if row[col_names.index("gs_value")] is not None:
                            authority_facet_singals.append(
                                FacetSignal(row[col_names.index("gsid")], row[col_names.index("gs_value")],
                                            row[col_names.index("fsid")],
                                            embedding=sken_singleton.Singletons.get_instance().perform_embeddings(
                                                row[col_names.index("gs_value")]),
                                            embedding_method=constants.fetch_constant("encoding_method")))
                        else:
                            authority_facet_singals.append(
                                FacetSignal(row[col_names.index("gsid")], row[col_names.index("gs_value")],
                                            row[col_names.index("fsid")],
                                            embedding=None,
                                            embedding_method=None))
                    elif str(row[col_names.index("facet_name")]).lower() == "budget":
                        b_id = row[col_names.index("fsid")]
                        if row[col_names.index("gs_value")] is not None:
                            budget_facte_singals.append(
                                FacetSignal(row[col_names.index("gsid")], row[col_names.index("gs_value")],
                                            row[col_names.index("fsid")],
                                            embedding=sken_singleton.Singletons.get_instance().perform_embeddings(
                                                row[col_names.index("gs_value")]),
                                            embedding_method=constants.fetch_constant("encoding_method")))
                        else:
                            budget_facte_singals.append(
                                FacetSignal(row[col_names.index("gsid")], row[col_names.index("gs_value")],
                                            row[col_names.index("fsid")],
                                            embedding=None,
                                            embedding_method=None))
                    elif str(row[col_names.index("facet_name")]).lower() == "interest":
                        i_id = row[col_names.index("fsid")]
                        if row[col_names.index("gs_value")] is not None:
                            interest_face_signals.append(
                                FacetSignal(row[col_names.index("gsid")], row[col_names.index("gs_value")],
                                            row[col_names.index("fsid")],
                                            embedding=sken_singleton.Singletons.get_instance().perform_embeddings(
                                                row[col_names.index("gs_value")]),
                                            embedding_method=constants.fetch_constant("encoding_method")))
                        else:
                            interest_face_signals.append(
                                FacetSignal(row[col_names.index("gsid")], row[col_names.index("gs_value")],
                                            row[col_names.index("fsid")],
                                            embedding=None,
                                            embedding_method=None))
                    else:
                        n_id = row[col_names.index("fsid")]
                        if row[col_names.index("gs_value")] is not None:
                            need_facet_singals.append(
                                FacetSignal(row[col_names.index("gsid")], row[col_names.index("gs_value")],
                                            row[col_names.index("fsid")],
                                            embedding=sken_singleton.Singletons.get_instance().perform_embeddings(
                                                row[col_names.index("gs_value")]),
                                            embedding_method=constants.fetch_constant("encoding_method")))
                        else:
                            need_facet_singals.append(
                                FacetSignal(row[col_names.index("gsid")], row[col_names.index("gs_value")],
                                            row[col_names.index("fsid")],
                                            embedding=None,
                                            embedding_method=None))

            with ThreadPoolExecutor(max_workers=6) as executor:
                executor.submit(sken_singleton.Singletons.get_instance().set_cached_intro_dims, "key_value_proposition",
                                Facet(kvp_id, "key value proposition", kvp_facet_signals))
                executor.submit(sken_singleton.Singletons.get_instance().set_cached_intro_dims, "aspiration_setting",
                                Facet(as_id, "aspiration setting", as_facet_signals))
                executor.submit(sken_singleton.Singletons.get_instance().set_cached_lq_dims, "authority",
                                Facet(a_id, "authority", authority_facet_singals))
                executor.submit(sken_singleton.Singletons.get_instance().set_cached_lq_dims, "budget",
                                Facet(b_id, "budget", budget_facte_singals))
                executor.submit(sken_singleton.Singletons.get_instance().set_cached_lq_dims, "interest",
                                Facet(i_id, "interest", interest_face_signals))
                executor.submit(sken_singleton.Singletons.get_instance().set_cached_lq_dims, "need_investigation",
                                Facet(n_id, "need investigation", need_facet_singals))

            logger.info("Cached {} facet signals for org={} and product={} in {}".format(len(
                kvp_facet_signals + as_facet_signals + authority_facet_singals + interest_face_signals + budget_facte_singals + need_facet_singals),
                org_id, prod_id, (time.time() - start)))

        else:
            logger.info("No facet_signals found for organization={} and product={}".format(org_id, prod_id))
            raise sken_exceptions.NoFacetFound(org_id,prod_id)
    else:
        logger.info("Skipping caching of facet_signals for organization={} and product_id ={}, they already exist in RAM".format(org_id,prod_id))


def wraper_method(input_file_path, org_id, product_id, threshold):
    vad_chunks = snippet_service.make_snippets(input_file_path)
    if len(snippets) !=0:
        try:
            make_cached_dimensions(org_id,product_id)

        lead_qualification_vad_chunks =[]
        for vad_chunk in vad_chunks:
            if snippet_service.check_snippet_speaker(vad_chunk):
                logger.info("Speaker =Agent for ")

