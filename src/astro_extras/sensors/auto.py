# Astro SDK Extras project
# (c) kol, 2023

from airflow.sensors.external_task import ExternalTaskSensor

class AutoOnlyTaskSensor(ExternalTaskSensor):
    """ A DAG communication operator that takes into account the way DAG is launched.

    If the pending DAG was started manually, the operator will not wait for the expected DAG to complete
    and the execution will continue immediately.
    """

    def execute(self, context):
        """ Primary operator execution method """
        if context['run_id'].find('manual') == 0:
            self.log.info('Manual execution detected, skipping DAG wait')
        else:
            super().execute(context)
