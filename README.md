# French Municipal Elections 2026 — Gender Bias in Media Coverage

A data analysis portfolio project examining whether French media coverage of the 2026 municipal elections (*élections municipales*) shows systematic differences in how male and female candidates are reported on.

---

## Research Questions

- Do French news outlets cover male and female list leaders at different rates during the 2026 municipal election cycle?
- When coverage does exist, does the framing and tone differ by candidate gender?
- Are observed differences associated with factors such as city size, political affiliation, or incumbent status?

---

## Context

France held its municipal elections on **15 and 22 March 2026**. With over 900,000 candidates across more than 50,000 lists, these elections are one of the largest democratic exercises in the country. Since 2026, all communes — including those under 1,000 inhabitants — are required to use party-list balloting with strict gender-alternation rules, making this a particularly relevant cycle for studying gender representation in both politics and media.

This project focuses on **list leaders** (*têtes de liste*) as the unit of analysis, keeping the scope tractable while capturing the candidates most likely to receive media attention.

---

## Analysis Scope

| Dimension | Detail |
|---|---|
| Analysis window | 1 February – 30 April 2026 |
| Candidates | 10–30 list leaders, balanced 50/50 male/female |
| Sampling | Stratified by city size, geographic region, political affiliation, and incumbent status |
| Language | French-language sources |
| Geography | France (metropolitan and overseas) |

The sample is intentionally small and stratified rather than exhaustive, to allow for deeper per-candidate analysis while remaining reproducible and auditable.

---

## Data Sources

All primary data comes from official French public open data, used under their respective open licences. Sources include:

- **Candidate and list data** — French Interior Ministry (*Ministère de l'Intérieur*) via [data.gouv.fr](https://www.data.gouv.fr), covering first-round candidate lists for the 2026 municipal elections. Data may be updated following legal challenges or corrections; version metadata is tracked.
- **Geographic reference data** — INSEE Code Officiel Géographique (COG) 2026, providing commune/département/région codes and labels used as join keys.
- **Seat and population data** — Interior Ministry dataset on council seat counts and population figures per commune, used for normalising exposure metrics.
- **Incumbent labels** — Interior Ministry RNE (*Répertoire National des Élus*) data on current mayors and councillors, used to flag incumbent candidates.
- **News data** — French-language news articles covering the election period, collected in compliance with applicable access rules and data minimisation principles. Only derived features and limited contextual excerpts are retained; full article text is not stored or published.

---

## Key Metrics (planned)

The analysis is organised around three layers:

1. **Exposure** — article counts, headline mentions, and number of distinct media sources per candidate, normalised by commune population.
2. **Tone and framing** — sentiment signals and topical frame distribution (e.g. policy/governance vs. appearance/private life) for sentences associated with each candidate.
3. **Bias indicators** — gender-level comparisons of framing distributions and stereotype-associated vocabulary frequency, with regression models controlling for city size, political bloc, incumbent status, and region.

---

## Ethical and Legal Notes

- All official datasets are published under open licences; sources and version timestamps are recorded.
- News article collection follows the principle of **data minimisation**: only fields required for the analysis are retained.
- No full article texts are stored, redistributed, or displayed publicly.
- French TDM (text and data mining) rules and CNIL recommendations are respected.

---

## Status

> **Early stage.** The project is currently in the planning and data exploration phase. Implementation details — tooling, pipeline architecture, and modelling choices — are being finalised.

---

## Project Structure

```
election-gender-bias_D4W/
  README.md
  plan/               # Research design and technical planning documents
  data/               # Data files (not committed to git)
  src/                # Source code (forthcoming)
  tests/              # Tests (forthcoming)
```

---

## Motivation

This project is being developed as a portfolio piece demonstrating end-to-end data work: from ingesting and modelling structured official data, to collecting and analysing unstructured text, to producing interpretable quantitative findings. The 2026 municipal elections provide a timely, well-defined, and publicly documented empirical setting.
