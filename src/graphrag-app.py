from langchain_neo4j import Neo4jGraph, GraphCypherQAChain
from langchain_ollama.llms import OllamaLLM

class GraphRAGApp:
    def __init__(self, uri, auth):
        self.uri = uri
        self.auth = auth
        self.graph = Neo4jGraph(url=uri, username=auth[0], password=auth[1])
        self.llm = OllamaLLM(model="mistral:7b-instruct", base_url="http://ollama:11434")
        self.chain = GraphCypherQAChain.from_llm(
            self.llm, 
            graph=self.graph, 
            verbose=True,
            allow_dangerous_requests=True
        )
    
    def chat_with_rag(self, query):
        return self.chain.run(query)

