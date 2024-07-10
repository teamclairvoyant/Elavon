import sys

from dq_rule_execution_engine.src.executor import execute

if __name__ == "__main__":
    execute(sys.argv[1])
