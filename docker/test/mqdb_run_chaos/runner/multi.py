# This file defines a MyScaleClient class with multiple locust task functions
# including insert, delete, update and vector search.
# It will randomly execute each task in multiple processes mode
# when you execute it with the locust command.

import random
from locust import HttpUser, task, events
import clickhouse_connect
import pandas as pd
from helper.utils import logger

@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--cluster-host", type=str, default="clickhouse-chaos-test", help="host")
    parser.add_argument("--cluster-port", type=int, default=8123, help="port number")


class MyScaleClient(HttpUser):
    counter = 999999
    dim = 960
    table_name = "gist1m"

    def on_start(self):
        """
        initialize the clickhouse client
        """
        self.client = clickhouse_connect.get_client(
            host=self.environment.parsed_options.cluster_host,
            port=self.environment.parsed_options.cluster_port,
            username='default',
            password=''
        )

    def _query(self, vector, top_k):
        """
        execute vector search
        """
        query = f"""
                    SELECT id, distance('alpha=1')(vector, {vector}) as dist FROM {self.table_name} ORDER BY dist DESC LIMIT {top_k}
                """
        return self.client.query(query).named_results()

    def _update(self, id, dataframe):
        """
        update data by deleting and inserting operations
        """
        self._delete(id)
        self._insert(dataframe=dataframe)

    def _delete(self, id):
        """
        delete one row data
        """
        query = f"DELETE FROM default.{self.table_name} where id = {id}"
        return self.client.command(query)

    def _delete_range(self, ids):
        """
        delete a list of data
        """
        query = f"DELETE FROM default.{self.table_name} where id in {ids}"
        return self.client.command(query)

    def _insert(self, dataframe):
        """
        insert data
        """
        self.client.insert_df(table=self.table_name, df=dataframe, database="default")

    @task
    def search_vector(self):
        """
        the task for vector search
        """
        vector = []
        for i in range(self.dim):
            vector.append(random.random())
        try:
            self._query(vector=vector, top_k=100)
        except Exception as e:
            logger.error(f"Unexpected exception {str(e)}")

    @task
    def insert_vector(self):
        """
        the task for insert one row of data into the table
        """
        vector = []
        for i in range(self.dim):
            vector.append(random.random())
        data = {'id': [self.counter], 'vector': [vector]}
        try:
            dataframe = pd.DataFrame(data)
            self._insert(dataframe)
            self.counter = self.counter + 1
        except Exception as e:
            logger.error(f"Unexpected exception {str(e)}")

    @task
    def update_vector(self):
        """
        the task for update vector data
        """
        vector = []
        for i in range(self.dim):
            vector.append(random.random())
        if self.counter <= 0:
            return
        data = {'id': [self.counter], 'vector': [vector]}
        try:
            self._update(id=self.counter, dataframe=pd.DataFrame(data))
            self.counter = self.counter + 1
        except Exception as e:
            logger.error(f"Unexpected exception {str(e)}")

    @task
    def insert_vectors(self):
        """
        the task for insert one hundred rows of data into the table
        """
        count = 100
        vector = []
        for i in range(self.dim):
            vector.append(random.random())
        data = {'id': [], 'vector': []}
        for i in range(count):
            data["id"].append(self.counter)
            data["vector"].append(vector)
            self.counter += 1
        try:
            self._insert(dataframe=pd.DataFrame(data))
        except Exception as e:
            logger.error(f"Unexpected exception {str(e)}")
            self.counter -= count

    @task
    def delete_vector_1(self):
        """
        the task for delete one row of data
        """
        if self.counter <= 0:
            return
        try:
            self._delete(id=(self.counter - 1))
            self.counter -= 1
        except Exception as e:
            logger.error(f"Unexpected exception {str(e)}")

    @task
    def delete_vector_100(self):
        """
        the task for delete one hundred rows of data
        """
        count = 100
        if self.counter <= 0:
            return
        elif self.counter < count:
            ids = [i for i in range(self.counter)]
        else:
            ids = [i for i in range(count)]
        try:
            self._delete_range(ids)
        except Exception as e:
            logger.error(f"Unexpected exception {str(e)}")
