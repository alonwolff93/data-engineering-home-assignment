from etl_code.compute_objectives import main
from etl_code.etl_utils import start_spark_session, ETLArgs, INPUT_PATH, OUTPUT_PATH
import os
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def compute_objectives_runner():
    """
    Creating inputs for ETL main function and call it

    Initializes input Spark session
    Initializes args
    Stops Spark session on completion
    """
    log.info("Starting Spark Session")
    spark_session = start_spark_session()
    args = ETLArgs(
        input_path=INPUT_PATH,
        output_path_obj_1= os.path.join(OUTPUT_PATH, "objective_1/"),
        output_path_obj_2=os.path.join(OUTPUT_PATH, "objective_2/"),
        output_path_obj_3=os.path.join(OUTPUT_PATH, "objective_3/"),
        output_path_obj_4=os.path.join(OUTPUT_PATH, "objective_4/"),
    )
    try:
        log.info("Starting ETL")
        main(spark_session, args, log)
    except Exception as e:
        log.error(f"ETL Failed: {e}")
        raise
    finally:
        spark_session.stop()
        log.info("Spark Session Stopped")


compute_objectives_runner()