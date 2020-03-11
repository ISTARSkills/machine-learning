import numpy as np
import pandas as pd
from src.utilities.objects import VadChunk
from src.utilities import sken_singleton, sken_logger, constants
from src.services import question_detection

logger = sken_logger.get_logger("snippet_service")


def agent_customer_sequence(input_excel_file):
    df = pd.read_excel(input_excel_file)
    print("!!!!!!!!!!!!!!!!!!!!!!")
    print(input_excel_file)
    print(df.columns)
    df.text_ = df.text.astype(str)
    df['a_bin'] = 0
    df['b_bin'] = 0
    df.a_bin = df.speaker.apply(lambda x: 0 if x == 'Agent' else 1)
    df.b_bin = df.speaker.apply(lambda x: 0 if x == 'Customer' else 1)
    df['a_bin_cumsum'] = df.a_bin.cumsum()
    df['b_bin_cumsum'] = df.b_bin.cumsum()
    df = df.drop(['a_bin', 'b_bin'], axis=1)
    df['a_bin'] = df.speaker.apply(lambda x: 1 if x == 'Agent' else 0)
    df['b_bin'] = df.speaker.apply(lambda x: 1 if x == 'Customer' else 0)
    df['a_con'] = df.a_bin_cumsum * df.a_bin
    df['b_con'] = df.b_bin_cumsum * df.b_bin
    df.drop(['a_bin_cumsum', 'b_bin_cumsum', 'a_bin', 'b_bin'], axis=1, inplace=True)
    df['identifier'] = df.a_con + df.b_con
    df['name_idnet'] = df.speaker + "_" + df.identifier.astype(str)
    df.drop(['a_con', 'b_con'], axis=1, inplace=True)
    df['text_'] = df['text'] + ". "
    df1 = df[['name_idnet', 'text_']].groupby(['name_idnet'], as_index=False).sum()
    df2 = df.drop_duplicates("name_idnet")[['speaker', 'name_idnet']]
    df2 = df2.merge(df1, on='name_idnet')
    df2 = df2.drop(["name_idnet"], axis=1)
    df2['text_'] = df2.text_.apply(lambda x: x.strip("."))
    return df2


def make_snippets(df, snippet_ids, task_id):
    if len(df) != 0:
        sentences = df["text_"].to_list()
        sentence_vectors = sken_singleton.Singletons.get_instance().perform_embeddings(sentences)
        vad_chunks = []
        for i in range(len(df)):
            vad_chunks.append(
                VadChunk(snippet_ids[i], None, None, df["speaker"][i], df["text_"][i], np.array([sentence_vectors[i]]),
                         None, task_id, questions=None,
                         q_encoding=None,
                         encoding_method=constants.fetch_constant("encoding_method")))
        return vad_chunks
    else:
        return []


def check_snippet_speaker(vad_chunk):
    """
    This method checks if the snippet speaker is agent or customer
    :param vad_chunk:
    :return: true if speaker is agent else false
    """
    return True if vad_chunk.speaker == "Agent" else False


def find_snippet_questions(vad_chunk):
    """
    Extractes questions from snippet text and sets the snippet question if any question is detected else sets if to None
    :param vad_chunk:
    :return: None
    """
    questions = question_detection.find_questions(vad_chunk.text)
    if len(questions) != 0:
        logger.info("Found {} question for snippet_id={}".format(len(questions), vad_chunk.sid))
        vad_chunk.set_questions(questions)
    else:
        logger.info("Did not find any question in snippet {}".format(vad_chunk.sid))
        vad_chunk.set_questions(None)


def make_snippet_question_embeddings(vad_chunk):
    """
    Sets the sentence embedding of snippet questions if present else sets it to None
    :param vad_chunk:
    :return: None
    """
    if vad_chunk.questions is not None:
        vad_chunk.set_question_encoding(
            sken_singleton.Singletons.get_instance().perform_embeddings(vad_chunk.questions),
            constants.fetch_constant("encoding_method"))
        logger.info(
            "Calculated  embeddings for {} snippet questions for snippet_id ={}".format(
                len(vad_chunk.questions),
                vad_chunk.sid))
    else:
        logger.info("There were not snippet questions for snippet_id={}".format(vad_chunk.sid))
        vad_chunk.set_question_encoding(None, None)


if __name__ == "__main__":
    make_snippets("/home/andy/Downloads/snippets_test.xlsx")
