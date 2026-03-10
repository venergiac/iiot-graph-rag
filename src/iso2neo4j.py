from neo4j import GraphDatabase, Result
import json

class PlantDataImporter:
    def __init__(self, uri, user, password, database="neo4j"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database
    
    def close(self):
        self.driver.close()

    def clean_all_data(self):
        with self.driver.session() as session:
            session.run(
                """MATCH (n)
                DETACH DELETE n""")
    
    def import_plant_data(self, json_file):
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        with self.driver.session() as session:
            # Create company
            company_name = data['company_info']['corporate']
            session.run(
                "CREATE (c:Company {id: $id, name: $company_name, industry: $industry, standard: $standard})",
                id=company_name,
                company_name=company_name.lower(),
                industry=data['company_info']['industry'],
                standard=data['company_info']['standard_reference']
            )

            
            
            # Create installation
            installation_name=data['installation_data']['level_3_installation']
            session.run(
                """
                MATCH (c:Company {id: $company_name})
                CREATE (i:Installation {id: $id, name: $installation_name, location: $location, context: $context})
                CREATE (c)-[:HAS_INSTALLATION]->(i)
                """,
                company_name=company_name,
                id=installation_name,
                installation_name=installation_name.lower(),
                location=data['installation_data']['location'],
                context=data['installation_data']['operating_context']
            )
            
            # Create equipment and measurements
            for equipment in data['equipment_inventory']:
                eq_id = equipment['equipment_id']
                tech = equipment['technical_data']
                
                session.run(
                    """MATCH (i:Installation {id: $installation_name})
                        CREATE (e:Equipment {
                        id: $id, name: $id, manufacturer: $mfg, model: $model, 
                        operating_mode: $mode, criticality: $crit
                    })
                    CREATE (i)-[:HAS_EQUIPMENT]->(e)
                    """,
                    installation_name=installation_name,
                    id=eq_id,
                    name=eq_id.lower(),
                    mfg=tech.get('manufacturer'), 
                    model=tech.get('model'), mode=tech.get('operating_mode'),
                    crit=tech.get('criticality', 'Unknown')
                )
                
                # Create measurements
                for measurement in equipment.get('current_measurements', []):
                    session.run(
                        """MATCH (e:Equipment {id: $eq_id})
                           CREATE (m:Measurement {name: $param_name, value: $val, unit: $unit, status: $status})
                           CREATE (e)-[:HAS_MEASUREMENT]->(m)""",
                        eq_id=eq_id, 
                        param_name=measurement['parameter'],
                        val=measurement['value'], unit=measurement.get('unit'),
                        status=measurement.get('status', 'Unknown')
                    )
            
            # Create failure events
            for failure in data['reliability_data']['failure_events']:
                session.run(
                    """MATCH (e:Equipment {id: $eq_id})
                       CREATE (f:FailureEvent {
                           id: $fail_id, mode: $mode, impact: $impact, 
                           mechanism: $mechanism, down_time_hrs: $down_time
                       })
                       CREATE (e)-[:EXPERIENCED_FAILURE]->(f)""",
                    eq_id=failure['equipment_ref'], fail_id=failure['event_id'],
                    mode=failure['failure_details']['failure_mode_desc'],
                    impact=failure['failure_details']['failure_impact'],
                    mechanism=failure['failure_details']['failure_mechanism'],
                    down_time=failure['maintenance_impact']['down_time_hrs']
                )

    def get_equipments(self):
        return self.driver.execute_query(
            "MATCH (c:Company)-[:HAS_INSTALLATION]->(i:Installation)-[:HAS_EQUIPMENT]->(e:Equipment) RETURN c.id +'.'+ i.id + '.' + e.id AS EQUIPMENT",
            database_=self.database,
            result_transformer_=Result.to_df
        )