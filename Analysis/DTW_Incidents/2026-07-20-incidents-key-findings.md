# Incidents Key Findings

***Incident Rate by Model Type and Connection***
Conversion Model
- criteo 82%
- facebook: 50%
- google: 35%
- tiktok: 0%

Causal Model
- criteo: 97%
- facebook: 50%
- google: Not enough data

Note: They are way higher than in the past. This is also due to the fact, that we monitor now all treatment - signals and not just the ones with control group settings. But I think a deep dive into this is necessary as well.

<!-- TODO: Why are the criteo incidents so high, but hardly any tickets were created -->

***Weekly Incident Rates***

Conversion Model: Decreasing trend since week 25.
Causal: Peak in week 27, but otherwise pretty constant

![Weekly incidents by model type](2026-07-22-analyse/weekly_incidents_overall_model_type.png)

![Monthly incidents by model type](2026-07-22-analyse/monthly_incidents_workspaces_connection.png)

<!-- TODO: Why the peak for the causal models? -->

***Changes Applied***
no_tags                                    48
Incident: Nothing                          35
No Performance Access                      17
Deactivated                                13
Incident: low spend                        12
Incident: Fluctuations                     11
DTW False Positive                         10
Incident: Hyperparamter                     6
Incident: Model                             5
Incident: Testing                           2
DTW True Positive|Incident: Nothing         2
Incident: Nothing|Incident: low spend       1
Onboarding                                  1
Onboarding|DTW False Positive               1


Note:
- no_tags are tickets which are still in progress
- No Performance Access: e.g. for KFZTeile. We can validate it over Google, but we do not have access to their tool. This is a problem, because we see different vlaues.
- Incident: low spend: If low spend is registered (but not because of a drop), e.g. for ective this is often the case.
- Incident: Model - pretty low, compare to the other ones.


![Tags for conversion model](2026-07-22-analyse/weekly_tags_conversion.png)
- No real tendency in handling most of the issues.
- The big reconstruction for the exclusions is not listed here. -> Add deep dive into models/query

![Tags for causal model](2026-07-22-analyse/weekly_tags_causal.png)

***Duration of Incident Periods by Model Type***
- causal: 37 days (non incident: 8 days)
- conversion: 11 days (non incident periods: 9 days)

![Duration of incident periods](2026-07-22-analyse/incidents_by_definition_duration.png)

Open Questions / Deep Dive:
- Why are the criteo incidents so high, but hardly any tickets were created?
- Why are the causal models worse in performance? And why is this not represented in the Pylon Tickets? Probably, because for each treatment one signal is chosen. If a treatment has several signals, then probably only the conversion signal is chosen.
- No action types from Automated Incident Handling?


***Fine-Tuning***
Based on the LLM Evaluation
- size (via usageLog) -> if empty, highlight
- comparison with other, well working, signal settings -> changing hyperparamters as lookbackdays -> trigger via pylon workflow

PMAX:
- missing search themes: if empty, highlight (needs to be added, to usage log, if possible)
- check of distribution upload (if possible, how much goes into youtube, display, ... )
    - ? what is the optimal traffic distribution
