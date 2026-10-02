# Repository layout

```text
channel-manager/
├── package.json                         # pnpm workspace and Turbo scripts
├── pnpm-workspace.yaml                  # workspace package globs
├── turbo.json                           # build/test task graph
├── tsconfig.base.json                   # strict Node 20 / TypeScript defaults
├── docker-compose.yml                   # local PostgreSQL, Redis, Kafka, OTel
├── atlas.hcl                            # Atlas versioned SQL migration config
├── .github/workflows/ci.yml              # lint, typecheck, test, build
├── docs/
│   ├── architecture.md                   # Part 1 architecture reference
│   ├── failure-modes.md                  # recovery checklist
│   ├── runbook.md                        # operational runbook outline
│   ├── repository-layout.md              # this tree
│   └── decisions/                        # Kafka and Redis-lock ADRs
├── packages/
│   ├── hospitality-contracts/             # Zod and AsyncAPI shared contracts
│   │   ├── src/events/                    # CloudEvents type declarations
│   │   ├── src/schemas/                   # Reservation and ARI schemas
│   │   └── asyncapi.yaml
│   ├── db/                                # PostgreSQL migration/query package
│   │   ├── migrations/                    # ordered Atlas versioned SQL migrations
│   │   ├── seeds/                         # local development seed data
│   │   ├── roles-and-grants.sql           # app and worker roles, grants, tenant notes
│   │   ├── tests/                         # pgTAP schema tests
│   │   └── src/                           # Kysely types and tenant-context helper
│   └── shared/                            # logger, OTel, config, Kafka/Redis clients
├── services/
│   ├── ingestion-gateway/                 # Fastify ingress and sanitization boundary
│   ├── normalizer/                        # raw to canonical event consumer
│   ├── pollers/                           # pull and explicit-ack workers
│   ├── core-state-engine/                 # transactional state and outbox relay
│   ├── outbound-engine/                   # channel delivery scheduling
│   ├── reconciler/                        # scheduled drift reconciliation
│   └── admin-api/                         # dashboard read API
├── adapters/
│   ├── base/                              # shared adapter contract
│   ├── booking-com/                       # partner-gated adapter package
│   ├── expedia/                           # Partner Central/EQC, not Rapid
│   ├── airbnb/                            # partner-gated adapter package
│   └── pms/                               # OHIP, Mews, Cloudbeds adapter family
├── apps/
│   └── testing-dashboard/                 # Next.js 14 dev/test dashboard
├── tests/
│   ├── integration/                       # Testcontainers integration coverage
│   └── load/                              # k6 workload scripts
└── infra/
    ├── docker/                            # shared one-service-per-image Dockerfile pattern
    ├── helm/                              # Kubernetes deployment charts
    └── terraform/                         # optional AWS infrastructure (not provisioned)
```

Business logic and adapter-specific external contracts are **OUT OF SCOPE (Part 2B)**.
