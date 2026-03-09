from langchain_neo4j import Neo4jGraph, GraphCypherQAChain
from langchain_ollama.llms import OllamaLLM

class OllamaGraphRAG:
    def __init__(self, uri, username, password, ollama_url="http://ollama:11434", model="mistral:7b-instruct"):
        self.graph = Neo4jGraph(url=uri, username=username, password=password)
        self.llm = OllamaLLM(model=model, base_url=ollama_url)
        self.chain = GraphCypherQAChain.from_llm(
            self.llm, 
            graph=self.graph, 
            verbose=True,
            allow_dangerous_requests=True
        )
    
    def chat_with_rag(self, query):
        return self.chain.invoke(query)

