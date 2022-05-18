from airflow.operators.http_operator import SimpleHttpOperator

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook


from airflow.plugins_manager import AirflowPlugin


class MultiQueryHttpOperator(SimpleHttpOperator):

    def __init__(
        self,
        *,
        queries: list = [],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.queries = queries

    def execute(self, context):
        """
        Using same code from http.py and making request for all queries
        """

        from airflow.utils.operator_helpers import determine_kwargs

        http = HttpHook(self.method, http_conn_id=self.http_conn_id, auth_type=self.auth_type)

        self.log.info("Calling HTTP method")

        responses = []

        for query in self.queries:

            response = http.run(self.endpoint + "?" + query, self.data, self.headers, self.extra_options)
            if self.log_response:
                self.log.info(response.text)
            if self.response_check:
                kwargs = determine_kwargs(self.response_check, [response], context)
                if not self.response_check(response, **kwargs):
                    raise AirflowException("Response check returned False.")
            if self.response_filter:
                kwargs = determine_kwargs(self.response_filter, [response], context)
                responses.append(self.response_filter(response, **kwargs))
            responses.append(response.text)

        return responses

class MultiQueryHttpPlugin(AirflowPlugin):
    name = "http_plugin"
    operators = [MultiQueryHttpOperator]
