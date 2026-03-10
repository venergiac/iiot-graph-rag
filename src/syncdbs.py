import neo4j

class SyncDBs:

    def __init__(self, influxdb_client, neo4j_driver, org="iiot", bucket="measures") -> None:
        self.influxdb_client = influxdb_client
        self.neo4j_driver = neo4j_driver
        self.bucket = bucket
        self.org = org


    def get_value(self, measurement, field, time_back):

        query_api = self.influxdb_client.query_api()
        query = f"""from(bucket:"{self.bucket}")
                    |> range(start: {time_back})
                    |> filter(fn:(r) => r._measurement == "{measurement}")
                    |> filter(fn:(r) => r._field == "{field}")
                    |> top(n: 1)"""
        result = query_api.query(org=self.org, query=query)
        results = []
        for table in result:
            for record in table.records:
                results.append((record.get_field(), record.get_value()))

        if len(results) <=0:
            print(f"Empy result {measurement} {field}", results)
            return None

        return results[0][1]

    def update_value(self, equipment, field, value):
        with self.neo4j_driver.session() as session:
            session.run("MATCH (m:Measurement {name: $field}) SET m.value = toFloat($value) RETURN m",
                        equipment=equipment,
                        field=field, 
                        value=value)
        
        print(equipment, field, "updated to value=", value)

    def sync(self, time_back="-20y"):

        with self.neo4j_driver.session() as session:

            # read all mesures
            mesurements = self.neo4j_driver.execute_query(
                "MATCH (c:Company)-[:HAS_INSTALLATION]->(i:Installation)-[:HAS_EQUIPMENT]->(e:Equipment)-[:HAS_MEASUREMENT]->(m:Measurement) RETURN c.id +'.'+ i.id + '.' + e.id AS EQUIPMENT, m.name AS FIELD",
                database_="neo4j",
                result_transformer_=neo4j.Result.to_df
            )

            #store on neo4j
            for _i, _row in mesurements.iterrows():

                equipment = _row["EQUIPMENT"]
                field = _row["FIELD"]

                value = self.get_value(equipment, field, time_back)

                if value is not None:
                    self.update_value(equipment, field, value)
            