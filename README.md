# ⬡ AegisGraph — Autonomous Multi-Agent Context Engine on FalkorDB

> **Graph Hacks (FalkorDB × WeMakeDevs) Hackathon Submission**  
> **Tracks Entered:**  
> - 🕵️ **Track 01:** Agents That Act on Connected Data  
> - 🤖 **Track 02:** Agent Memory and Coordination  
> - 🧠 **Track 03:** Company Brain  

---

## 1. Executive Summary & Why FalkorDB Is Essential (The "Rip-Out Test")

Production Site Reliability Engineering (SRE) and software systems fail when incidents happen because **root causes, financial blast radii, and institutional memories live in complex, multi-hop relationships** — not isolated text chunks.

Flat Vector RAG and traditional relational databases fail during real incidents:
* **Vector databases** compute text similarity; they cannot traverse a 4-hop chain connecting a failing server to downstream code functions and customer financial contracts.
* **Relational SQL databases** require expensive, multi-table recursive `JOIN`s that degrade under high-concurrency incident triage.

If you remove **FalkorDB** from AegisGraph and replace it with PostgreSQL or Pinecone, the application completely breaks because it relies on:
1. **Track 1 (Connected Data):** Native graph algorithms (`algo.pageRank`, `algo.WCC`, `shortestPath`) and multi-hop Cypher traversals (`MATCH (root:Microservice)<-[:DEPENDS_ON*1..3]-(upstream)-[:IMPACTS_SLA]->(sla)`) executing in sub-milliseconds on every agent loop.
2. **Track 2 (Agent Memory):** Bi-temporal edge invalidation (`valid_at` and `invalid_at` timestamps on semantic facts) and multigraph tenant isolation (`aegis_memory_acme`, `aegis_memory_globex`) executing 50+ concurrent multi-threaded reads and writes.
3. **Track 3 (Company Brain):** Traversing complete 6-hop lineage from microservices to AST code functions, Jira tickets, superseded architecture decisions, internal RFC documents, and on-call engineers.

---

## 2. System Architecture & Multi-Graph Topology

AegisGraph uses a single FalkorDB instance hosting isolated multigraph namespaces:

### Graph 1: `aegis_enterprise_brain` (Tracks 1 & 3)
```text
(:CustomerSLA)<-[:IMPACTS_SLA]-(:Microservice)-[:DEPENDS_ON*1..3]->(:Microservice)
                                      │                                  │
                                 [:OWNED_BY]                        [:EXECUTES]
                                      ▼                                  ▼
                                  (:Team)<-[:MEMBER_OF]-(:Engineer) (:CodeFunction)-[:CALLS]->(:CodeFunction)
                                                            │                                       ▲
                                                   [:AUTHORED_BY / RESOLVED]              [:MODIFIES_FUNCTION]
                                                            ▼                                       │
(:RFC_Document)<-[:RECORDED_IN]-(:ArchitectureDecision)-[:SUPERSEDES]->(:ArchitectureDecision) (:JiraTicket)
```

### Graph 2: `aegis_memory_<tenant_id>` (`acme`, `globex`, `umbrella` — Track 2)
```text
(:Agent)-[:HANDOFF_STATE {status, last_handoff_at}]->(:Agent)
   │
   ├──[:RECORDED_EPISODE]──► (:EpisodicMemory {session_id, timestamp, event})
   │                                │
   │                    [:CONFIRMED_FACT / :INVALIDATED_FACT]
   │                                ▼
   └──► (:SemanticFact {entity, statement, valid_at, invalid_at})-[:SUPERSEDED_FACT]->(:SemanticFact)

(:ProceduralStep {step_order: 1})-[:NEXT_STEP]->(:ProceduralStep {step_order: 2})-[:NEXT_STEP]->(:ProceduralStep {step_order: 3})
```

---

## 3. Core Cypher Queries & Graph Algorithms (Rule 10 Compliance)

### A. Track 1: Structural Criticality via PageRank & WCC
```cypher
// Compute node criticality across the microservice mesh
CALL algo.pageRank('Microservice', 'DEPENDS_ON')
YIELD node, score
RETURN node.name AS service, node.tier AS tier, score
ORDER BY score DESC;

// Detect isolated component clusters
CALL algo.WCC('Microservice', 'DEPENDS_ON')
YIELD node, componentId
RETURN node.name AS service, componentId;

// Dynamic bi-directional critical path
MATCH p = (a:Microservice {name: $svc})-[:DEPENDS_ON*1..3]->(b:Microservice)
RETURN [n IN nodes(p) | n.name] AS path;
```

### B. Track 1: Multi-Hop SLA Blast Radius & AST Bottlenecks
```cypher
MATCH (root:Microservice {name: $svc})<-[:DEPENDS_ON*0..3]-(upstream:Microservice)-[:IMPACTS_SLA]->(sla:CustomerSLA)
OPTIONAL MATCH (root)-[:EXECUTES]->(fn1:CodeFunction)-[:CALLS*1..2]->(fn2:CodeFunction)
RETURN DISTINCT
  upstream.name           AS impacted_service,
  sla.customer            AS customer,
  sla.contract_arr_usd    AS arr_at_risk_usd,
  sla.penalty_per_min_usd AS penalty_per_min,
  fn1.name                AS entry_function,
  fn2.name                AS bottleneck_function,
  fn2.file                AS file_path,
  fn2.complexity          AS cyclomatic_complexity;
```

### C. Track 3: Company Brain Provenance & Supersedes Lineage
```cypher
MATCH (s:Microservice {name: $svc})<-[:AFFECTED]-(inc:Incident)-[:RESOLVED_BY_ENGINEER]->(resolver:Engineer)
OPTIONAL MATCH (s)-[:EXECUTES]->(:CodeFunction)-[:CALLS*1..2]->(fn:CodeFunction)
      <-[:MODIFIES_FUNCTION]-(jira:JiraTicket)<-[:IMPLEMENTED_IN]-(dec_new:ArchitectureDecision)
OPTIONAL MATCH (dec_new)-[:SUPERSEDES]->(dec_old:ArchitectureDecision)-[:RECORDED_IN]->(rfc_old:RFC_Document)
OPTIONAL MATCH (dec_new)-[:RECORDED_IN]->(rfc_new:RFC_Document)-[:AUTHORED_BY]->(author:Engineer)-[:MEMBER_OF]->(team:Team)
RETURN inc.id, inc.root_cause, resolver.name, dec_old.summary, rfc_old.id, dec_new.summary, rfc_new.id, author.name, team.slack, jira.id, fn.name;
```

### D. Track 2: Bi-Temporal Semantic Fact Invalidation
```cypher
OPTIONAL MATCH (old:SemanticFact {entity: $entity, invalid_at: 'ACTIVE'})
SET old.invalid_at = $now
CREATE (new_f:SemanticFact {id: $new_id, entity: $entity, statement: $stmt, valid_at: $now, invalid_at: 'ACTIVE'})
FOREACH (_ IN CASE WHEN old IS NOT NULL THEN [1] ELSE [] END |
  CREATE (new_f)-[:SUPERSEDED_FACT]->(old)
)
RETURN coalesce(old.id, 'none') AS invalidated_id, new_f.id AS active_id;
```

---

## 4. One-Command Setup & Local Deployment (Rule 10 & Rule 4 Compliance)

The hackathon guidelines explicitly accept complete local setup instructions with Docker. Spin up the entire system with a single command:


```bash
# 0. With my repo link just clone and run!!! 
git clone https://github.com/VivekChowdary7/AegisGraph.git
cd AegisGraph
chmod +x start.sh
./start.sh

# 1. Clean hidden characters and make start.sh executable
python3 -c "import pathlib; [p.write_text(p.read_text(encoding='utf-8').replace('\u00a0', ' ').replace('\u200b', ''), encoding='utf-8') for p in pathlib.Path('.').glob('*') if p.is_file()]"
chmod +x start.sh

# 2. Run the master start script
./start.sh
```

### Access Points
* **Mission Control UI (Interactive Dashboard):** `http://localhost:8000`
* **FalkorDB Visual Browser:** `http://localhost:3000`
* **Official JSON-RPC 2.0 MCP Endpoint:** `POST http://localhost:8000/mcp`
* **Interactive API Documentation:** `http://localhost:8000/docs`

---

## 5. Development Tooling & AI Assistance Statement (Rule 11 Compliance)

In accordance with Rule 11 of the Graph Hacks Official Rules, AI assistance was used strictly as an accelerator tool to speed up manual typing, formatting, and UI component boilerplate. 

AI was utilized solely as a velocity multiplier — not as a replacement for engineering intellect. The core domain design, multi-graph architecture, bi-temporal schema, multi-hop Cypher queries, MCP tool dispatching, and grounded abstention algorithms were conceived, engineered, and verified by the author.
