# Databricks Data Engineer Associate — Career Path

A hands-on training that prepares you for the **Databricks Certified Data Engineer Associate**
exam (version **May 4, 2026** — 45 questions · 90 minutes · 7 domains) by building a full
**medallion lakehouse** (bronze → silver → gold) over a synthetic, intentionally-dirty
**insurance** dataset, then publishing dashboards. Start at Chapter 0.

![Medallion Lineage](https://raw.githubusercontent.com/msg-CareerPaths/databricks-training/main/diagrams/medallion-lineage.svg "Medallion Lineage")


## Getting Started

The training material lives at:
**<https://github.com/msg-CareerPaths/databricks-training>**

Clone it to your local machine before you begin:

```bash
git clone https://github.com/msg-CareerPaths/databricks-training.git
cd databricks-training
```

Then open Chapter 0 and follow the setup instructions there.

---

## Mandatory Content

 - [0. Introduction](https://github.com/msg-CareerPaths/databricks-training/blob/main/chapters/000-introduction.md)
 - [1. Platform Foundations](https://github.com/msg-CareerPaths/databricks-training/blob/main/chapters/100-platform-foundations.md)
 - [2. Generate & Land the Data](https://github.com/msg-CareerPaths/databricks-training/blob/main/chapters/200-generate-and-land.md)
 - [3. Bronze — Ingestion & Loading](https://github.com/msg-CareerPaths/databricks-training/blob/main/chapters/300-bronze-ingestion.md)
 - [4. Silver — Transform & Conform](https://github.com/msg-CareerPaths/databricks-training/blob/main/chapters/400-silver-transform.md)
 - [5. Gold — Dimensional Modeling](https://github.com/msg-CareerPaths/databricks-training/blob/main/chapters/500-gold-modeling.md)
 - [6. Lakeflow Declarative Pipelines](https://github.com/msg-CareerPaths/databricks-training/blob/main/chapters/600-declarative-pipelines.md)
 - [7. Orchestration with Lakeflow Jobs](https://github.com/msg-CareerPaths/databricks-training/blob/main/chapters/700-lakeflow-jobs.md)
 - [8. CI/CD — Git Folders & Bundles](https://github.com/msg-CareerPaths/databricks-training/blob/main/chapters/800-cicd-bundles.md)
 - [9. Troubleshooting, Monitoring & Optimization](https://github.com/msg-CareerPaths/databricks-training/blob/main/chapters/900-troubleshooting-optimization.md)
 - [10. Governance & Security](https://github.com/msg-CareerPaths/databricks-training/blob/main/chapters/950-governance-security.md)
 - [11. Dashboards & Exam Readiness](https://github.com/msg-CareerPaths/databricks-training/blob/main/chapters/990-dashboards-and-readiness.md)

## Optional Content

There is no particular order in which you have to choose to begin with them.

 - [OPT-1. Lakeflow Connect](https://github.com/msg-CareerPaths/databricks-training/blob/main/chapters/opt-100-lakeflow-connect.md)
 - [OPT-2. Structured Streaming Deep-Dive](https://github.com/msg-CareerPaths/databricks-training/blob/main/chapters/opt-200-streaming-deep-dive.md)
 - [OPT-3. Performance & Liquid Clustering](https://github.com/msg-CareerPaths/databricks-training/blob/main/chapters/opt-300-performance-liquid-clustering.md)
 - [OPT-4. CI with GitHub Actions](https://github.com/msg-CareerPaths/databricks-training/blob/main/chapters/opt-400-cicd-github-actions.md)

---

## Commit Style

Use [Conventional Commits](https://www.conventionalcommits.org/) for changes to this
training repository:

```text
<type>(<optional scope>): <short description>

<optional body explaining why the change is needed>
```

| Type | Use for | Example |
| --- | --- | --- |
| `feat` | New training exercises or lakehouse capabilities | `feat(bronze): add claims ingestion exercise` |
| `fix` | Corrections to notebooks, SQL, pipelines, or setup | `fix(silver): handle null policy dates` |
| `chore` | Maintenance, dependencies, or bundle configuration | `chore(bundle): update dev target settings` |
| `refactor` | Restructuring code without changing behavior | `refactor(gold): share date dimension logic` |
| `docs` | README, chapters, and studybook changes | `docs(chapters): clarify Unity Catalog setup` |
| `test` | Adding or updating automated tests | `test(generator): cover missing claim fields` |

Use an imperative subject ("add", not "added"), keep it under 50 characters,
and omit the trailing period. Make one logical change per commit. If a body is
needed, explain *why* the change matters and wrap lines at 72 characters.
When working from a GitHub issue, reference it in the body (for example,
`Closes #42`); no issue is required for a standalone training exercise.

### Branch Naming

Branch from `main`. Use lowercase, hyphen-separated words and the same type
as your commit. Include the issue number when there is one:

```text
<type>/<issue-number>-<short-description>
# Examples: feat/42-bronze-claims-ingestion, fix/57-silver-null-dates
```

For practice work without an issue, omit the number (for example,
`docs/unity-catalog-setup`). Keep branch names short and descriptive.

---

> **Building the project (mentors & self-learners):** the deep theory + code companion is the
> [studybook](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/studybook/00_index.md);
> environment setup, **repository layout, and quickstart** are in the
> [project guide](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/00_project_guide.md).
> This repo also ships a working **data generator**, **worked examples**, and a deployable
> **Declarative Automation Bundle**.
