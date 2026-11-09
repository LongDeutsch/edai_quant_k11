# ==============================================================================
# Module: 06_agents.mk - LLM Inference, AgentGateway & Multi-Agent Fleet (Phase 7)
# ==============================================================================

# --- [ LLM & Multi-Agent Fleet ] ---

mcp-serve: ## Start containerized FastAPI MCP tools service (Phase 7)
	@echo "Notice: Target '$@' is scheduled for Phase 7."
	@echo "Refer to docs/plan/IMPLEMENTATION_PLAN.md#phase-7-rag--llm--mcp--multi-agent for technical design."
	@exit 1

agent-coordinator: ## Run Coordinator Agent orchestrating investment briefings (Phase 7)
	@echo "Notice: Target '$@' is scheduled for Phase 7."
	@echo "Refer to docs/plan/IMPLEMENTATION_PLAN.md#phase-7-rag--llm--mcp--multi-agent for technical design."
	@exit 1

rag-index: ## Index financial news and filings into Qdrant vector database (Phase 7)
	@echo "Notice: Target '$@' is scheduled for Phase 7."
	@echo "Refer to docs/plan/IMPLEMENTATION_PLAN.md#phase-7-rag--llm--mcp--multi-agent for technical design."
	@exit 1

.PHONY: mcp-serve agent-coordinator rag-index
