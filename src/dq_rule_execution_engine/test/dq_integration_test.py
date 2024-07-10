from pathlib import Path
import findspark
from dq_rule_execution_engine.src.executor import execute


findspark.init()
app_conf_file_path = Path(__file__).parent / "conf/properties/application.properties"
ruleset_conf_file_path = Path(__file__).parent / "C:/Users/Kamal199261/PycharmProjects/data-quality-batch-dev/dq_rule_execution_engine/test/conf/json/local/order_data_null_check.json"

rule_set_path = str(ruleset_conf_file_path)
app_conf = str(app_conf_file_path)
execute(f'rule_set_path={rule_set_path},app_conf={app_conf},job_id=12345678')
