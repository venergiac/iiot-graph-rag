from langchain_neo4j import Neo4jGraph, GraphCypherQAChain
from langchain_ollama.llms import OllamaLLM
from langchain_core.prompts import PromptTemplate



class OllamaGraphRAG:

    def __init__(self, uri, username, password, ollama_url="http://ollama:11434", model="mistral:7b-instruct"):
        self.graph = Neo4jGraph(url=uri, username=username, password=password)
        self.llm = OllamaLLM(model=model, base_url=ollama_url)

        CYPHER_GENERATION_TEMPLATE = """
        Task:Generate Cypher statement to query a graph database.
        Instructions:
        Use only the provided relationship types and properties in the schema.
        Do not use any other relationship types or properties that are not provided.

        Cypher examples:
        # What is the value of Bearing Temperature of PUMP-001?
        MATCH (c:Company)-[:HAS_INSTALLATION]->(i:Installation)-[:HAS_EQUIPMENT]->(e:Equipment)
        WHERE e.id = 'PUMP-001'
        OPTIONAL MATCH (e)-[:HAS_MEASUREMENT]->(m:Measurement)
        WHERE m.name = 'Bearing Temperature'
        RETURN m.value

        Note: Do not include any explanations or apologies in your responses.
        Do not respond to any questions that might ask anything else than for you to construct a Cypher statement.
        Do not include any text except the generated Cypher statement.

        The question is:
        {question}"""

        CYPHER_GENERATION_PROMPT = PromptTemplate(
            input_variables=["question"], template=CYPHER_GENERATION_TEMPLATE
        )


        self.chain = GraphCypherQAChain.from_llm(
            self.llm, 
            graph=self.graph, 
            verbose=True,
            allow_dangerous_requests=True,
            cypher_prompt=CYPHER_GENERATION_PROMPT
        )
    
    def chat_with_rag(self, query):
        return self.chain.invoke(query)

