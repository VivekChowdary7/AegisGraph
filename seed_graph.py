"""
AegisGraph — Triple-Crown FalkorDB Graph Seeder (v3.1 Complete Lineage)
Seeds isolated multi-tenant graphs with full multi-hop paths for all services.
"""
import os
from falkordb import FalkorDB


def seed_all():
    db = FalkorDB(
        host=os.getenv("FALKORDB_HOST", "localhost"),
        port=int(os.getenv("FALKORDB_PORT", 6379)),
    )

    # =========================================================================
    # GRAPH 1: ENTERPRISE BRAIN & CONNECTED INFRASTRUCTURE (TRACKS 1 & 3)
    # =========================================================================
    print("🔷 Seeding aegis_enterprise_brain (Tracks 1 & 3)...")
    g_brain = db.select_graph("aegis_enterprise_brain")
    try:
        g_brain.delete()
    except Exception:
        pass
    g_brain = db.select_graph("aegis_enterprise_brain")

    brain_cypher = """
    CREATE
      // ── Teams & Engineers (Track 3: Who owns what & who knows what) ──
      (t_core:Team {name: 'Core Platform', slack: '#core-platform'}),
      (t_pay:Team  {name: 'Payments & Risk', slack: '#payments-eng'}),
      (t_edge:Team {name: 'Edge & Traffic', slack: '#edge-sre'}),

      (e_arjun:Engineer {id: 'eng_1', name: 'Arjun Mehta', role: 'Staff SRE',           on_call: true}),
      (e_sara:Engineer  {id: 'eng_2', name: 'Sara Chen',   role: 'Principal Architect', on_call: false}),
      (e_dev:Engineer   {id: 'eng_3', name: 'Vikram Rao',  role: 'Backend Lead',        on_call: true}),

      (e_arjun)-[:MEMBER_OF]->(t_core),
      (e_sara)-[:MEMBER_OF]->(t_core),
      (e_dev)-[:MEMBER_OF]->(t_pay),

      // ── Microservices (Track 1: Connected Data) ──
      (s_auth:Microservice     {name: 'auth-token-svc',        tier: 'Tier-0', language: 'Go',     latency_ms: 420}),
      (s_gateway:Microservice  {name: 'api-gateway',           tier: 'Tier-0', language: 'Rust',   latency_ms: 18}),
      (s_checkout:Microservice {name: 'checkout-orchestrator', tier: 'Tier-1', language: 'Python', latency_ms: 890}),
      (s_ledger:Microservice   {name: 'ledger-writer',         tier: 'Tier-1', language: 'Java',   latency_ms: 45}),
      (s_notif:Microservice    {name: 'webhook-dispatcher',    tier: 'Tier-2', language: 'NodeJS', latency_ms: 110}),

      // ── Code AST Functions across all 3 primary services ──
      (fn_verify:CodeFunction   {name: 'validate_jwt_signature()', file: 'src/auth/jwt.go',       line: 142, complexity: 19}),
      (fn_pool:CodeFunction     {name: 'acquire_redis_conn()',     file: 'src/auth/pool.go',      line: 88,  complexity: 24}),
      (fn_route:CodeFunction    {name: 'proxy_upstream_grpc()',    file: 'src/gateway/proxy.rs',  line: 210, complexity: 16}),
      (fn_retry:CodeFunction    {name: 'backoff_retry_loop()',     file: 'src/gateway/retry.rs',  line: 64,  complexity: 22}),
      (fn_pay:CodeFunction      {name: 'authorize_card_hold()',    file: 'src/checkout/hold.py',  line: 115, complexity: 21}),
      (fn_idem:CodeFunction     {name: 'lock_idempotency_key()',   file: 'src/checkout/lock.py',  line: 49,  complexity: 27}),

      // ── Customer SLAs (Track 1: Financial Blast Radius) ──
      (sla_ent:CustomerSLA     {customer: 'GlobalBank Corp', contract_arr_usd: 2400000, max_latency_ms: 300, penalty_per_min_usd: 5000}),
      (sla_retail:CustomerSLA  {customer: 'MegaMart Retail', contract_arr_usd: 850000,  max_latency_ms: 500, penalty_per_min_usd: 1500}),
      (sla_fintech:CustomerSLA {customer: 'FinTech Startup', contract_arr_usd: 320000,  max_latency_ms: 800, penalty_per_min_usd: 600}),

      // ── Multi-hop Service Dependencies ──
      (s_gateway)-[:DEPENDS_ON  {weight: 0.95, call_type: 'gRPC'}]->(s_auth),
      (s_checkout)-[:DEPENDS_ON {weight: 0.90, call_type: 'gRPC'}]->(s_auth),
      (s_checkout)-[:DEPENDS_ON {weight: 0.85, call_type: 'Kafka'}]->(s_ledger),
      (s_ledger)-[:DEPENDS_ON   {weight: 0.60, call_type: 'HTTP'}]->(s_notif),

      // ── Code-level execution & call edges ──
      (s_auth)-[:EXECUTES]->(fn_verify),
      (fn_verify)-[:CALLS]->(fn_pool),
      (s_gateway)-[:EXECUTES]->(fn_route),
      (fn_route)-[:CALLS]->(fn_retry),
      (s_checkout)-[:EXECUTES]->(fn_pay),
      (fn_pay)-[:CALLS]->(fn_idem),

      // ── SLA Exposure ──
      (s_checkout)-[:IMPACTS_SLA]->(sla_ent),
      (s_gateway)-[:IMPACTS_SLA]->(sla_retail),
      (s_checkout)-[:IMPACTS_SLA]->(sla_fintech),

      // ── Ownership & Code Maintainers ──
      (s_auth)-[:OWNED_BY]->(t_core),
      (s_gateway)-[:OWNED_BY]->(t_edge),
      (s_checkout)-[:OWNED_BY]->(t_pay),
      (e_arjun)-[:MAINTAINED_CODE {commits: 47}]->(fn_pool),
      (e_sara)-[:MAINTAINED_CODE  {commits: 31}]->(fn_retry),
      (e_dev)-[:MAINTAINED_CODE   {commits: 54}]->(fn_idem),

      // ── Track 3: Company Brain (RFCs, Decisions, Incidents, Jira) ──
      (rfc_104:RFC_Document {id: 'RFC-104', title: 'Static Auth Pool Sizing',              url: 'https://docs.internal/rfc-104', date: '2026-06-12'}),
      (rfc_119:RFC_Document {id: 'RFC-119', title: 'Dynamic Bursting & Token Verification', url: 'https://docs.internal/rfc-119', date: '2026-09-28'}),
      (rfc_201:RFC_Document {id: 'RFC-201', title: 'Gateway Unbounded Retry Policy',        url: 'https://docs.internal/rfc-201', date: '2026-07-04'}),
      (rfc_215:RFC_Document {id: 'RFC-215', title: 'Adaptive Jitter & Load Shedding',       url: 'https://docs.internal/rfc-215', date: '2026-10-02'}),
      (rfc_301:RFC_Document {id: 'RFC-301', title: 'Synchronous DB Idempotency Locks',      url: 'https://docs.internal/rfc-301', date: '2026-08-14'}),
      (rfc_312:RFC_Document {id: 'RFC-312', title: 'Async Redis Lease Idempotency',         url: 'https://docs.internal/rfc-312', date: '2026-10-09'}),

      (dec_auth_old:ArchitectureDecision {id: 'DEC-01', summary: 'Cap Redis pool at 50 connections to save memory',                                  status: 'SUPERSEDED'}),
      (dec_auth_new:ArchitectureDecision {id: 'DEC-02', summary: 'Enable dynamic pool bursting up to 500 connections during flash sales',            status: 'ACTIVE'}),
      (dec_gw_old:ArchitectureDecision   {id: 'DEC-03', summary: 'Retry failed upstream gRPC calls 5x immediately without backoff',                  status: 'SUPERSEDED'}),
      (dec_gw_new:ArchitectureDecision   {id: 'DEC-04', summary: 'Enforce exponential backoff with 20% jitter and trip circuit-breaker at 502 rate', status: 'ACTIVE'}),
      (dec_chk_old:ArchitectureDecision  {id: 'DEC-05', summary: 'Acquire blocking Postgres row lock inside lock_idempotency_key()',                 status: 'SUPERSEDED'}),
      (dec_chk_new:ArchitectureDecision  {id: 'DEC-06', summary: 'Switch idempotency lock to non-blocking FalkorDB/Redis lease with 300ms TTL',      status: 'ACTIVE'}),

      (inc_402:Incident {id: 'INC-402', title: 'Auth Pool Exhaustion under 10k RPS', root_cause: 'Static 50-connection limit in acquire_redis_conn()',       resolved_by: 'Increase pool_max to 500 and enable circuit breaker'}),
      (inc_305:Incident {id: 'INC-305', title: 'Gateway 502 Retry Storm',            root_cause: 'Unbounded synchronous retries in backoff_retry_loop()',   resolved_by: 'Enable exponential jitter and shed load per RFC-215'}),
      (inc_511:Incident {id: 'INC-511', title: 'Checkout Deadlock on Flash Sale',    root_cause: 'Blocking row lock contention in lock_idempotency_key()', resolved_by: 'Migrate to non-blocking lease lock per RFC-312'}),

      (jira_88:JiraTicket {id: 'PLAT-882', title: 'Migrate auth-token-svc to dynamic pool config',      status: 'In Review'}),
      (jira_91:JiraTicket {id: 'EDGE-910', title: 'Cap gRPC retries and add jitter in backoff_retry',   status: 'In Progress'}),
      (jira_95:JiraTicket {id: 'PAY-954',  title: 'Replace blocking SQL lock in lock_idempotency_key', status: 'Ready to Merge'}),

      // ── Lineage for auth-token-svc ──
      (dec_auth_old)-[:RECORDED_IN]->(rfc_104),
      (dec_auth_new)-[:RECORDED_IN]->(rfc_119),
      (dec_auth_new)-[:SUPERSEDES {reason: 'Caused INC-402 under high load'}]->(dec_auth_old),
      (rfc_119)-[:AUTHORED_BY]->(e_sara),
      (inc_402)-[:AFFECTED]->(s_auth),
      (inc_402)-[:RESOLVED_BY_ENGINEER]->(e_arjun),
      (dec_auth_new)-[:IMPLEMENTED_IN]->(jira_88),
      (jira_88)-[:MODIFIES_FUNCTION]->(fn_pool),

      // ── Lineage for api-gateway ──
      (dec_gw_old)-[:RECORDED_IN]->(rfc_201),
      (dec_gw_new)-[:RECORDED_IN]->(rfc_215),
      (dec_gw_new)-[:SUPERSEDES {reason: 'Triggered INC-305 retry amplification'}]->(dec_gw_old),
      (rfc_215)-[:AUTHORED_BY]->(e_sara),
      (inc_305)-[:AFFECTED]->(s_gateway),
      (inc_305)-[:RESOLVED_BY_ENGINEER]->(e_sara),
      (dec_gw_new)-[:IMPLEMENTED_IN]->(jira_91),
      (jira_91)-[:MODIFIES_FUNCTION]->(fn_retry),

      // ── Lineage for checkout-orchestrator ──
      (dec_chk_old)-[:RECORDED_IN]->(rfc_301),
      (dec_chk_new)-[:RECORDED_IN]->(rfc_312),
      (dec_chk_new)-[:SUPERSEDES {reason: 'Caused INC-511 checkout deadlock'}]->(dec_chk_old),
      (rfc_312)-[:AUTHORED_BY]->(e_dev),
      (inc_511)-[:AFFECTED]->(s_checkout),
      (inc_511)-[:RESOLVED_BY_ENGINEER]->(e_dev),
      (dec_chk_new)-[:IMPLEMENTED_IN]->(jira_95),
      (jira_95)-[:MODIFIES_FUNCTION]->(fn_idem)
    """
    g_brain.query(brain_cypher)
    print("  ✅ Enterprise Brain graph seeded (full lineage for all 3 services).")

    # =========================================================================
    # GRAPH 2: BI-TEMPORAL AGENT MEMORY & SWARM COORDINATION (TRACK 2)
    # =========================================================================
    print("🔷 Seeding aegis_memory_acme (Track 2)...")
    g_mem = db.select_graph("aegis_memory_acme")
    try:
        g_mem.delete()
    except Exception:
        pass
    g_mem = db.select_graph("aegis_memory_acme")

    memory_cypher = """
    CREATE
      (ag_triage:Agent {name: 'Triage-Agent-01', role: 'Blast Radius & Anomaly Detector'}),
      (ag_brain:Agent  {name: 'Brain-Agent-02',  role: 'Institutional Knowledge & Provenance'}),
      (ag_remedy:Agent {name: 'Remedy-Agent-03', role: 'Autonomous Remediation & Memory Writer'}),

      (ep_1:EpisodicMemory {session_id: 'sess_2026_10_01', timestamp: '2026-10-01T14:20:00Z', event: 'Detected spike on auth-token-svc; attempted pod restart which failed to clear connection lock.'}),
      (ep_2:EpisodicMemory {session_id: 'sess_2026_10_10', timestamp: '2026-10-10T09:15:00Z', event: 'Applied dynamic pool burst ENV_POOL_MAX=500; latency dropped from 420ms to 19ms. RFC-119 validated.'}),
      (ep_3:EpisodicMemory {session_id: 'sess_2026_10_13', timestamp: '2026-10-13T22:00:00Z', event: 'INC-305 cascaded from auth pool; gateway retry storm mitigated via RFC-215 jitter.'}),

      (f_stale:SemanticFact {
        id: 'fact_101',
        entity: 'auth-token-svc',
        statement: 'Best remediation for latency spike is rolling pod restart',
        valid_at:   '2026-05-01T00:00:00Z',
        invalid_at: '2026-10-01T14:25:00Z'
      }),
      (f_active:SemanticFact {
        id: 'fact_102',
        entity: 'auth-token-svc',
        statement: 'Rolling restart fails under lock contention; must patch acquire_redis_conn pool_max to 500 per RFC-119 and enable circuit-breaker',
        valid_at:   '2026-10-01T14:25:00Z',
        invalid_at: 'ACTIVE'
      }),
      (f_gateway:SemanticFact {
        id: 'fact_103',
        entity: 'api-gateway',
        statement: 'Gateway 502 storms require enabling 20% exponential backoff jitter in backoff_retry_loop() per RFC-215',
        valid_at:   '2026-10-13T22:05:00Z',
        invalid_at: 'ACTIVE'
      }),
      (f_checkout:SemanticFact {
        id: 'fact_104',
        entity: 'checkout-orchestrator',
        statement: 'Checkout latency spikes under flash sales require switching lock_idempotency_key() to 300ms non-blocking lease per RFC-312',
        valid_at:   '2026-10-14T11:00:00Z',
        invalid_at: 'ACTIVE'
      }),

      (p1:ProceduralStep {step_order: 1, action: 'Run PageRank + Blast Radius check on degraded service',          tool: 'mcp_run_graph_algorithms',           success_rate: 0.99}),
      (p2:ProceduralStep {step_order: 2, action: 'Verify active ArchitectureDecision in Company Brain graph',       tool: 'mcp_trace_company_brain_provenance', success_rate: 0.97}),
      (p3:ProceduralStep {step_order: 3, action: 'Execute RFC-verified remediation, page SME, persist episode',     tool: 'mcp_recall_and_commit_bitemporal_memory', success_rate: 0.95}),
      (p1)-[:NEXT_STEP {confidence: 1.0}]->(p2),
      (p2)-[:NEXT_STEP {confidence: 0.96}]->(p3),

      (ag_triage)-[:RECORDED_EPISODE]->(ep_1),
      (ag_remedy)-[:RECORDED_EPISODE]->(ep_2),
      (ag_brain)-[:RECORDED_EPISODE]->(ep_3),

      (ep_1)-[:INVALIDATED_FACT]->(f_stale),
      (ep_2)-[:CONFIRMED_FACT]->(f_active),
      (ep_3)-[:CONFIRMED_FACT]->(f_gateway),

      (ag_triage)-[:HANDOFF_STATE {session: 'sess_live', status: 'READY', payload: 'blast_radius_result'}]->(ag_brain),
      (ag_brain)-[:HANDOFF_STATE  {session: 'sess_live', status: 'READY', payload: 'provenance_result'}]->(ag_remedy)
    """
    g_mem.query(memory_cypher)
    print("  ✅ Bi-Temporal Memory graph seeded.")
    print("\n🎉 All graphs seeded! Open http://localhost:3000 in FalkorDB Browser to verify.")


if __name__ == "__main__":
    seed_all()
