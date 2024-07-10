import random
from dq_rule_execution_engine.src.utils import read_file


class AppContext:
    def __init__(self, run_time_params):
        self.run_time_params_dict = {}
        self.run_time_params = run_time_params
        self.ruleset_conf = None
        self.app_conf = None
        self.job_id = None
        self.current_rule = None

    def build(self):
        run_time_params_list = list(self.run_time_params.split(','))

        for entry in run_time_params_list:
            name, value = entry.split('=', 1)
            self.run_time_params_dict[name.strip()] = value.strip()

        self.ruleset_conf = read_file(self.run_time_params_dict['rule_set_path'])
        self.app_conf = read_file(self.run_time_params_dict['app_conf'])
        random_job_id = random.randint(10000000, 99999999)
        self.job_id = int(random_job_id)

    def get_value(self, key):
        return self.app_conf.get(key)

    def get_rules(self):
        return self.ruleset_conf['rules']

    def set_current_rule(self, rule):
        self.current_rule = rule

    def get_current_rule(self):
        return self.current_rule

    def get_rule_template_name(self):
        return self.get_current_rule()['rule_details']['template']['name']

    def get_rule_id(self):
        return int(self.get_current_rule()['rule_details']['id'])

    def get_rule_property(self, key):
        return [rule_property for rule_property in self.get_current_rule()['rule_details']['properties']
                if rule_property['key'] == key][0]['value']

    def get_template_property(self, key):
        return [rule_property for rule_property in self.get_current_rule()['rule_details']['template']['properties']
                if rule_property['key'] == key][0]['value']

    def is_key_exist_in_rule_property(self, key):
        return key in [rule_property['key'] for rule_property in self.get_current_rule()['rule_details']['properties']]

    def get_source_entity(self):
        return [entity for entity in self.get_current_rule()['rule_details']['data_entity_associations']
                if entity['entity_behaviour'].upper() == 'SOURCE'][0]

    def get_target_entity(self):
        return [entity for entity in self.get_current_rule()['rule_details']['data_entity_associations']
                if entity['entity_behaviour'].upper() == 'TARGET'][0]

    def get_ruleset_id(self):
        return int(self.ruleset_conf['ruleset_id'])

    def get_job_run_id(self):
        return self.job_id

    def get_entity_name(self):
        return self.ruleset_conf['name']

    def get_description(self):
        return self.ruleset_conf['description']

    def get_entity_physical_name(self):
        return [rule_property['entity_physical_name'] for rule_property in
                self.get_current_rule()['rule_details']['data_entity_associations']][0]
