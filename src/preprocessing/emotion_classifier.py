
import polars as pl
import duckdb
from src import global_configs as cf
from src.models import roberta_emotion
from src.utils import duckdb_utils

CONFIGS = cf.PREPROCESSING_CONFIGS["Text_Preprocessing_Configurations"]


def emotion_classification() -> None:
    """
    Classifies emotions from transcripts stored in a DuckDB database and updates the
    database with the classified emotion scores.

    This function performs the following steps:
    1. Connects to a DuckDB database and retrieves transcripts.
    2. Utilizes a pre-trained RoBERTa-based emotion classification model to classify
       emotions for each transcript.
    3. Converts the classified emotion data into a wide-format DataFrame.
    4. Merges the emotion classification results back with the original DataFrame.
    5. Updates the DuckDB database with the emotion classification results.

    Raises:
        RuntimeError: If database connection or data processing fails.
        KeyError: If expected configuration keys are missing.
    """

    # Create a DuckDB database connection and get the data from the database
    duckdb_database = cf.DATA_PATH.joinpath(CONFIGS["DuckDB_File"]).resolve()
    conn = duckdb.connect(database=duckdb_database)

    sql_string = f"SELECT ID, Transcript FROM {CONFIGS["Transcript_Source_Table"]};"
    transcripts_data = conn.sql(sql_string).pl().to_dicts()

    # Extract the transcript out and then classify each transcript with their emotion
    transcripts = [x["Transcript"] for x in transcripts_data]
    roberta = roberta_emotion.RobertaEmotion(
        model_name=CONFIGS["Emotion_Classifier_Model"]["Model_Name"],
        api_token=None
    )
    emotion_labels = roberta.emotion_classifier(transcripts)

    # Convert the data back to wide format
    emotion_rows = [
        {item["label"]: item["score"] for item in row}
        for row in emotion_labels
    ]
    emotion_df = pl.DataFrame(emotion_rows)

    # Combine the data back into the dataframe and insert into DuckDB
    result_df = pl.concat([pl.DataFrame(transcripts_data), emotion_df], how="horizontal")
    duckdb_utils.duckdb_insert(
        duckdb_engine=conn,
        schema_name=CONFIGS["Emotion_Classifier_Model"]["Output_Table"].split(".")[0],
        table_name=CONFIGS["Emotion_Classifier_Model"]["Output_Table"].split(".")[-1],
        df=result_df,
        full_load=True
    )
    conn.close()


if __name__ == "__main__":
    emotion_classification()
