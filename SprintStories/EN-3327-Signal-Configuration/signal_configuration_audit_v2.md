# Signal Configuration Audit

Generated: 2026-06-09 12:37 UTC

## Summary

- **Total active audiences:** 224
- **Correct configurations:** 181
- **Incorrect configurations:** 43

## Default Configurations

Reference: [Signal Default Setups](https://coda.io/d/_dLhoSqeSHRj/Signal-Update-Filters-IN-PROGRESS_su9pf4Ao#Signal-Default-Setups_tuUNl2Rm)

### Meta (`facebook`)

| Audience type | `targetingOutlookDays` | `audienceSize` | `audienceSizePercentage` | `conversionLag` |
| --- | --- | --- | --- | --- |
| Default | 90 | 150,000 | - | 180 (Seed #2) |
| Seed – Premium (`t0-10p`) | 30 | 150,000 | 0 - 10 | - |
| Seed – Growth (`t10-20p`) | 90 | 150,000 | 10 - 20 | - |
| Seed – Volume (`t20-30p`) | 90 | 150,000 | 20 - 30 | - |
| Seed – Custom tier (`t7-15p`, …) | 90 | 150,000 | max tier value / 100 | - |
| Value-based | 90 | - | 1 or `-` | - |
| Retargeting | 180 | 150,000 | 0.5 | - |

### Google Analytics (`googleAnalytics`)

| Audience type | `targetingOutlookDays` | `audienceSize` | `audienceSizePercentage` | `conversionLag` |
| --- | --- | --- | --- | --- |
| Default | 30 | 150,000 | 0.5 | 180 (Seed #2) |
| Retargeting | 180 | 150,000 | 0.5 | - |

### Criteo (`criteo`)

| Audience type | `targetingOutlookDays` | `audienceSize` | `audienceSizePercentage` | `conversionLag` |
| --- | --- | --- | --- | --- |
| Default | 30 | 150,000 | 0.5 | 180 (Seed #2) |
| Retargeting | 180 | 150,000 | 0.5 | - |

### TikTok (`tiktok`)

| Audience type | `targetingOutlookDays` | `audienceSize` | `audienceSizePercentage` | `conversionLag` |
| --- | --- | --- | --- | --- |
| Default | 30 | `-` | 0.5 | 180 (Seed #2) |
| Retargeting | 180 | `-` | 0.5 | - |

### Exclusions (all platforms)

| Signal / pattern | `targetingOutlookDays` | `audienceSize` | `audienceSizePercentage` | `exclude_visitors` |
| --- | --- | --- | --- | --- |
| Default | per signal / pattern | `-` | 0.5 or `-` (innkeepr-analytics uses 0.5 when unset) | from `customer_specifications.yaml` |
| `Innkeepr - 30d Visitors - Exclusion` | 30 | - | 0.1 | - |
| `Innkeepr - 30-90d Visitors - Exclusion` | 90 | - | 0.3 | 30 |
| `Innkeepr - 90-180d Visitors - Exclusion` | 180 | - | 0.5 | 90 |
| `30d Visitor` | 30 | - | 0.1 | - |
| `30d Visitors` | 30 | - | 0.1 | - |
| `30-90d Visitors` | 90 | - | 0.3 | 30 |
| `90-180d Visitors` | 180 | - | 0.5 | 90 |
| `360d Purchaser` | 180 | - | - | - |
| `Low AOV` | 180 | - | - | - |
| `90d Brand` | 90 | - | - | - |
| `Brand - Exclusion` | 90 | - | - | - |
| `General - Exclusion` | 180 | - | - | - |
| `Exclusion - Standard` | 180 | - | - | - |
| `Exclusion - Bestandskunden` | 180 | - | - | - |
| `Exclusion - 365d` | 365 | - | - | - |
| `Exclusion - Lifetime` | 730 | - | - | - |

`360d Purchaser` exclusions also allow `audienceSizePercentage` = `1`.

## exclusion

**43** active audiences (40 correct, 3 incorrect).

### Visitors

**18** visitor exclusion audiences (15 correct, 3 incorrect).

- **30d Visitors:** 9 audiences (8 correct, 1 incorrect)
- **30-90d Visitors:** 8 audiences (6 correct, 2 incorrect)
- **90-180d Visitors:** 1 audience (1 correct, 0 incorrect)

### facebook

**39** audiences (36 correct, 3 incorrect).

| Status | Workspace | `audience.id` | Audience | Source | `model.type` | `audience.treatments.count` | Outlook | Size % | Size | `exclude_visitors` | Result |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ❌ | ESN | 68dd4522120f694f539c286b | Innkeepr - 30-90d Visitors - Exclusion | facebook | conversion | 82 | 90 | 0.2 | - | 30 | audienceSizePercentage is 0.2, expected 0.3 or None; test setup |
| ✅ | ESN | 69c2c47af56dab97354f044d | Innkeepr - 30-90d Visitors - Exclusion - CH | facebook | conversion | 1 | 90 | - | - | 30 | Configuration matches defaults |
| ✅ | ESN | 69c2c46df56dab97354f0225 | Innkeepr - 30-90d Visitors - Exclusion - FR | facebook | conversion | 0 | 90 | - | - | 30 | Configuration matches defaults |
| ✅ | ESN | 69c2c41df56dab973544d7aa | Innkeepr - 30-90d Visitors - Exclusion - NL | facebook | conversion | 0 | 90 | - | - | 30 | Configuration matches defaults |
| ✅ | ESN | 68dd4512120f694f539c0faa | Innkeepr - 360d Purchaser - Exclusion | facebook | conversion | 82 | 180 | - | - | - | Configuration matches defaults |
| ✅ | ESN | 69c2c48ef56dab97354f0aef | Innkeepr - 360d Purchaser - Exclusion - CH | facebook | conversion | 1 | 180 | - | - | - | Configuration matches defaults |
| ✅ | ESN | 69c2c460f56dab973544ebac | Innkeepr - 360d Purchaser - Exclusion - FR | facebook | conversion | 0 | 180 | - | - | - | Configuration matches defaults |
| ✅ | ESN | 69c2c432f56dab973544de2f | Innkeepr - 360d Purchaser - Exclusion - NL | facebook | conversion | 0 | 180 | - | - | - | Configuration matches defaults |
| ❌ | Junglueck | 6a16fa6c1370504ba6c47dc8 | Innkeepr - 30-90d Visitors - Exclusion | facebook | conversion | 0 | 90 | 0.5 | - | 30 | audienceSizePercentage is 0.5, expected 0.3 or None |
| ✅ | Junglueck | 69a07da22568bd24bccc0012 | Innkeepr - 30d Visitors - Exclusion | facebook | conversion | 3 | 30 | 0.1 | - | - | Configuration matches defaults |
| ✅ | Junglueck | 69a07d7d2568bd24bca9a11e | Innkeepr - 360d Purchaser - Exclusion | facebook | conversion | 0 | 180 | 1 | - | - | Configuration matches defaults |
| ✅ | Junglueck | 6a16fa931370504ba6c485ad | Innkeepr - 90-180d Visitors - Exclusion | facebook | conversion | 0 | 180 | 0.5 | - | 90 | Configuration matches defaults |
| ✅ | Junglueck | 69a57165c756486c667fd8fa | Innkeepr - Low AOV - Exclusion | facebook | conversion | 0 | 180 | 0.5 | - | - | Configuration matches defaults |
| ✅ | LILLYDOO | 6811d9de879ec174109231bd | Innkeepr - Exclusion - Bestandskunden | facebook | conversion | 0 | 180 | - | - | - | Configuration matches defaults |
| ✅ | LILLYDOO | 6811db14879ec1741092565c | Innkeepr - Exclusion - Standard | facebook | conversion | 0 | 180 | - | - | - | Configuration matches defaults |
| ✅ | MissPompadour GmbH | 6a1efda51370504ba68c88f7 | Innkeepr - 30d Visitors - Exclusion | facebook | conversion | 0 | 30 | 0.1 | - | - | Configuration matches defaults |
| ✅ | MissPompadour GmbH | 6a1efdb61370504ba68c8ad0 | Innkeepr - 360d Purchaser - Exclusion | facebook | conversion | 0 | 180 | - | - | - | Configuration matches defaults |
| ✅ | More | 68e3fa6c120f694f53a63059 | Innkeepr - 30-90d Visitors - Exclusion | facebook | conversion | 0 | 90 | - | - | 30 | Configuration matches defaults |
| ✅ | More | 69417cf5a5250a665d7bca71 | Innkeepr - 30d Visitors - Exclusion - NL | facebook | conversion | 25 | 30 | 0.1 | - | - | Configuration matches defaults |
| ✅ | More | 69417d0ca5250a665d7bd0d8 | Innkeepr - 30d Visitors - Exclusion - UK | facebook | conversion | 0 | 30 | 0.1 | - | - | Configuration matches defaults |
| ✅ | More | 68e3fa51120f694f53a5f863 | Innkeepr - 360d Purchaser - Exclusion | facebook | conversion | 1 | 180 | - | - | - | Configuration matches defaults |
| ✅ | More | 69417ce0a5250a665d7bc47f | Innkeepr - 360d Purchaser - Exclusion - NL | facebook | conversion | 27 | 180 | - | - | - | Configuration matches defaults |
| ✅ | More | 69417d1ca5250a665d7bd58b | Innkeepr - 360d Purchaser - Exclusion - UK | facebook | conversion | 0 | 180 | - | - | - | Configuration matches defaults |
| ✅ | More | 69456cdea5250a665df58135 | Innkeepr - 90d Brand - Exclusion | facebook | conversion | 218 | 90 | - | - | - | Configuration matches defaults |
| ✅ | Nikin | 6847ea1c57bdaeb51ed9e5ec | Innkeepr - General - Exclusion | facebook | conversion | 0 | 180 | - | - | - | Configuration matches defaults |
| ❌ | Plantura | 699ea514f904fb1302a181f8 | Innkeepr - 30d Visitor - Exclusion | facebook | conversion | 0 | 90 | - | - | 30 | targetingOutlookDays is 90, expected 30; Consider renaming to `Innkeepr - 30-90d Visitors - Exclusion` (configuration matches `Innkeepr - 30-90d Visitors - Exclusion` setup) |
| ✅ | Plantura | 699ea502f904fb1302a17de7 | Innkeepr - 360d Purchaser - Exclusion | facebook | conversion | 0 | 180 | - | - | - | Configuration matches defaults |
| ✅ | Rosental | 6980c569f7564868fec26d22 | Innkeepr - 30d Visitors - Exclusion | facebook | conversion | 0 | 30 | - | - | - | Configuration matches defaults |
| ✅ | Rosental | 6980e30ff7564868fe593b93 | Innkeepr - 360d Purchaser - Exclusion | facebook | conversion | 0 | 180 | - | - | - | Configuration matches defaults |
| ✅ | Rosental | 6949736ba5250a665d429f1a | Innkeepr - Brand - Exclusion | facebook | conversion | 0 | 90 | 0.5 | - | - | Configuration matches defaults |
| ✅ | Rosental | 68302c5bef02595b6bea3cb5 | Innkeepr - Exclusion - 365d | facebook | conversion | 0 | 365 | - | - | - | Configuration matches defaults |
| ✅ | Rosental | 68302cd3ef02595b6bea4be9 | Innkeepr - Exclusion - Lifetime | facebook | conversion | 0 | 730 | - | - | - | Configuration matches defaults |
| ✅ | Rosental | 67bc16baaaa1eaa63251f771 | Innkeepr - Low AOV - Exclusion | facebook | conversion | 0 | 180 | - | - | - | Configuration matches defaults |
| ✅ | Störtebekker | 6a1dbe801370504ba657b511 | Innkeepr - 30-90d Visitors - Exclusion | facebook | conversion | 0 | 90 | - | - | 30 | Configuration matches defaults |
| ✅ | Störtebekker | 69ef864850b5382f00c19928 | Innkeepr - 30d Visitors - Exclusion | facebook | conversion | 0 | 30 | 0.1 | - | - | test setup onboarding |
| ✅ | Störtebekker | 69ef865450b5382f00c28675 | Innkeepr - 360d Purchaser - Exclusion | facebook | conversion | 0 | 180 | - | - | - | Configuration matches defaults |
| ✅ | ahead-nutrition.com | 6a2289061370504ba6f63d01 | Innkeepr - 30-90d Visitors - Exclusion | facebook | conversion | 0 | 90 | - | - | 30 | Configuration matches defaults |
| ✅ | ahead-nutrition.com | 6a2288ed1370504ba6f63ac8 | Innkeepr - 360d Purchaser - Exclusion | facebook | conversion | 0 | 180 | - | - | - | Configuration matches defaults |
| ✅ | to teach | 68454a0b57bdaeb51e6a10c5 | Innkeepr - Exclusion - Standard | facebook | conversion | 1 | 180 | 0.5 | - | - | Configuration matches defaults |

### googleAnalytics

**4** audiences (4 correct, 0 incorrect).

| Status | Workspace | `audience.id` | Audience | Source | `model.type` | `audience.treatments.count` | Outlook | Size % | Size | `exclude_visitors` | Result |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ✅ | ESN | 69e243e20d5be37c796337ba | Innkeepr - 30d Visitors - Exclusion | googleAnalytics | conversion | 0 | 30 | 0.1 | - | - | Configuration matches defaults |
| ✅ | ESN | 69e243c00d5be37c79632f6f | Innkeepr - 360d Purchaser - Exclusion | googleAnalytics | conversion | 0 | 180 | 0.5 | - | - | Configuration matches defaults |
| ✅ | MissPompadour GmbH | 6a1fca1d1370504ba6a56252 | Innkeepr - 30d Visitors - Exclusion | googleAnalytics | conversion | 0 | 30 | 0.1 | - | - | Configuration matches defaults |
| ✅ | MissPompadour GmbH | 6a1fca3b1370504ba6a5655a | Innkeepr - 360d Purchaser - Exclusion | googleAnalytics | conversion | 0 | 180 | 0.5 | - | - | Configuration matches defaults |

## retargeting

**21** active audiences (2 correct, 19 incorrect).

### Suggested default changes

A majority of signals in this type differ from the current defaults for the fields below:

- **`targetingOutlookDays`**: 17/21 signals use `90` vs current default `180` (19/21 mismatches). **Suggest new default: `90`**

### criteo

**16** audiences (0 correct, 16 incorrect).

| Status | Workspace | `audience.id` | Audience | Source | `model.type` | `audience.treatments.count` | Outlook | Size % | Size | `exclude_visitors` | Result |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ❌ | Tchibo | 696ded3ba5250a665d04adc2 | Innkeepr - Active - High Impact - RTG #1 | criteo | causal | 5 | 90 | 0.5 | 150000 | - | targetingOutlookDays is 90, expected 180 |
| ❌ | Tchibo | 696ded2fa5250a665d04aa65 | Innkeepr - Active - High Impact - RTG #2 | criteo | causal | 5 | 90 | 0.5 | 150000 | - | targetingOutlookDays is 90, expected 180 |
| ❌ | Tchibo | 696dec4fa5250a665d00c48f | Innkeepr - Active - Likely Purchaser  - RTG #1 | criteo | conversion | 4 | 90 | 0.5 | 150000 | - | targetingOutlookDays is 90, expected 180 |
| ❌ | Tchibo | 696dec8da5250a665d00d63d | Innkeepr - Active - Likely Purchaser  - RTG #2 | criteo | conversion | 4 | 90 | 0.5 | 150000 | - | targetingOutlookDays is 90, expected 180 |
| ❌ | Tchibo | 68b69f372e5f2d9a58cdd132 | Innkeepr - BabyKids - High Impact - Seed #1 | criteo | causal | 5 | 90 | 0.3 | 150000 | - | targetingOutlookDays is 90, expected 180; audienceSizePercentage is 0.3, expected 0.5 |
| ❌ | Tchibo | 68b69f7f2e5f2d9a58ce0829 | Innkeepr - BabyKids - High Impact - Seed #2 | criteo | causal | 5 | 90 | 0.3 | 150000 | - | targetingOutlookDays is 90, expected 180; audienceSizePercentage is 0.3, expected 0.5 |
| ❌ | Tchibo | 68b69dff2e5f2d9a58cce692 | Innkeepr - BabyKids - Likely Purchaser  - Seed #1 | criteo | conversion | 4 | 90 | 0.3 | 150000 | - | targetingOutlookDays is 90, expected 180; audienceSizePercentage is 0.3, expected 0.5 |
| ❌ | Tchibo | 68b69e392e5f2d9a58cd0e63 | Innkeepr - BabyKids - Likely Purchaser  - Seed #2 | criteo | conversion | 4 | 90 | 0.3 | 150000 | - | targetingOutlookDays is 90, expected 180; audienceSizePercentage is 0.3, expected 0.5 |
| ❌ | Tchibo | 68b69fe02e5f2d9a58ce5239 | Innkeepr - Body - High Impact - Seed #1 | criteo | causal | 6 | 90 | 0.5 | 150000 | - | targetingOutlookDays is 90, expected 180 |
| ❌ | Tchibo | 68b69ffe2e5f2d9a58ce69e6 | Innkeepr - Body - High Impact - Seed #2 | criteo | causal | 6 | 90 | 0.5 | 150000 | - | targetingOutlookDays is 90, expected 180 |
| ❌ | Tchibo | 68b69fa72e5f2d9a58ce2652 | Innkeepr - Body - Likely Purchaser - Seed #1 | criteo | conversion | 4 | 90 | 0.5 | 150000 | - | targetingOutlookDays is 90, expected 180 |
| ❌ | Tchibo | 68b69fb82e5f2d9a58ce3439 | Innkeepr - Body - Likely Purchaser - Seed #2 | criteo | conversion | 4 | 90 | 0.5 | 150000 | - | targetingOutlookDays is 90, expected 180 |
| ❌ | Tchibo | 69d8a8baaf22b56ab033b888 | Innkeepr - Ladies - Existing Customers - RTG #1 | criteo | conversion | 1 | 90 | 0.5 | 150000 | - | targetingOutlookDays is 90, expected 180 |
| ❌ | Tchibo | 69d8a8cfaf22b56ab033bfed | Innkeepr - Ladies - Existing Customers - RTG #2 | criteo | conversion | 1 | 90 | 0.3 | 150000 | - | targetingOutlookDays is 90, expected 180; audienceSizePercentage is 0.3, expected 0.5 |
| ❌ | Tchibo | 69d8a8e4af22b56ab033c6d2 | Innkeepr - Ladies - Likely Purchaser - RTG #1 | criteo | conversion | 1 | 90 | 0.5 | 150000 | - | targetingOutlookDays is 90, expected 180 |
| ❌ | Tchibo | 69d8a8eeaf22b56ab033c994 | Innkeepr - Ladies - Likely Purchaser - RTG #2 | criteo | conversion | 1 | 90 | 0.5 | 150000 | - | targetingOutlookDays is 90, expected 180 |

### facebook

**1** audiences (0 correct, 1 incorrect).

| Status | Workspace | `audience.id` | Audience | Source | `model.type` | `audience.treatments.count` | Outlook | Size % | Size | `exclude_visitors` | Result |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ❌ | More | 693048ca4227124153d15ae9 | Innkeepr - VMS Search - RTG | facebook | conversion | 0 | 90 | - | 150000 | - | targetingOutlookDays is 90, expected 180 |

### googleAnalytics

**4** audiences (2 correct, 2 incorrect).

| Status | Workspace | `audience.id` | Audience | Source | `model.type` | `audience.treatments.count` | Outlook | Size % | Size | `exclude_visitors` | Result |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ❌ | Pendix | 618bc646fb99125ac1c4f584 | Remarketing Innkeepr | googleAnalytics | causal | 14 | 250 | 0.6 | - | - | targetingOutlookDays is 250, expected 180; audienceSize is None, expected 150000; audienceSizePercentage is 0.6, expected 0.5 |
| ❌ | Pendix | 618bc646fb99125ac1c4f593 | Remarketing Innkeepr  Brand Consolidation 1 | googleAnalytics | conversion | 0 | 250 | 0.6 | - | - | targetingOutlookDays is 250, expected 180; audienceSize is None, expected 150000; audienceSizePercentage is 0.6, expected 0.5 |
| ✅ | Tchibo | 69f71c251ef3b58145b77693 | Innkeepr - Ladies - RTG #1 | googleAnalytics | conversion | 1 | 180 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Tchibo | 69f71c451ef3b58145f0bf1e | Innkeepr - Ladies - RTG #2 | googleAnalytics | conversion | 1 | 180 | 0.5 | 150000 | - | Configuration matches defaults |

## seed

**143** active audiences (122 correct, 21 incorrect).

### facebook

**98** audiences (90 correct, 8 incorrect).

| Status | Workspace | `audience.id` | Audience | Source | `model.type` | `audience.treatments.count` | Outlook | Size % | Size | `exclude_visitors` | Result |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ✅ | ESN | 693c07dda5250a665da38559 | Innkeepr - Bundles - Growth - Seed - t10-20p | facebook | conversion | 6 | 90 | 0.25 | 150000 | - | Configuration matches defaults |
| ✅ | ESN | 693c077ca5250a665da36c80 | Innkeepr - Bundles - Premium - Seed - t0-10p | facebook | conversion | 6 | 30 | 0.14 | 150000 | - | Configuration matches defaults |
| ✅ | ESN | 693c07eba5250a665da3892f | Innkeepr - Bundles - Volume - Seed - t20-30p | facebook | conversion | 6 | 90 | 0.36 | 150000 | - | Configuration matches defaults |
| ✅ | ESN | 69c2c346f56dab97354162de | Innkeepr - Creatin - Growth - Seed - t10-20p | facebook | conversion | 0 | 90 | 0.25 | 150000 | - | Configuration matches defaults |
| ✅ | ESN | 69c2c32df56dab9735415a5e | Innkeepr - Creatin - Premium - Seed - t0-10p | facebook | conversion | 0 | 30 | 0.14 | 150000 | - | Configuration matches defaults |
| ✅ | ESN | 69c2c356f56dab97354166fb | Innkeepr - Creatin - Volume - Seed - t20-30p | facebook | conversion | 0 | 90 | 0.36 | 150000 | - | Configuration matches defaults |
| ✅ | ESN | 693c0866a5250a665da3a7c4 | Innkeepr - Iso Clear - Premium - Seed - t0-10p | facebook | conversion | 1 | 30 | 0.14 | 150000 | - | Configuration matches defaults |
| ❌ | Innkeepr | 6a2749781370504ba691c3c2 | Innkeepr Facebook Seed | facebook | - | 1 | - | - | - | - | targetingOutlookDays is None, expected 90; audienceSize is None, expected 150000 |
| ✅ | Junglueck | 69a07f9e2568bd24bcd546d5 | Innkeepr - Kennenlernset - Growth - Seed - t10-20p | facebook | conversion | 11 | 90 | 0.3 | 150000 | - | Configuration matches defaults |
| ✅ | Junglueck | 69a07f6d2568bd24bcd5375d | Innkeepr - Kennenlernset - Premium - Seed - t0-10p | facebook | conversion | 11 | 30 | 0.15 | 150000 | - | Configuration matches defaults |
| ✅ | Junglueck | 69a07fb12568bd24bcd54c08 | Innkeepr - Kennenlernset - Volume - Seed - t20-30p | facebook | conversion | 11 | 90 | 0.4 | 150000 | - | Configuration matches defaults |
| ✅ | LILLYDOO | 69f33b0051f70b57e4d4d1de | Innkeepr - Abo - Seed | facebook | conversion | 1 | 90 | 0.5 | 150000 | - | Configuration matches defaults |
| ❌ | LILLYDOO | 6811dab9879ec17410924bf4 | Innkeepr - Seed - CHAT | facebook | conversion | 33 | 180 | - | 150000 | - | targetingOutlookDays is 180, expected 90 |
| ✅ | LILLYDOO | 6811db64879ec17410925ee0 | Innkeepr - Seed - LSTM | facebook | causal | 123 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | LILLYDOO | 6811dc2b879ec174109276ca | Innkeepr - Seed LSTM - ES | facebook | conversion | 60 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | LILLYDOO | 6811dc7e879ec174109280ba | Innkeepr - Seed LSTM - IT | facebook | conversion | 60 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | MissPompadour GmbH | 6a1efdf91370504ba68c933e | Innkeepr – Bestperformer – Seed – Growth t10–20 | facebook | conversion | 2 | 90 | - | 150000 | - | Configuration matches defaults |
| ❌ | MissPompadour GmbH | 6a1efde71370504ba68c916e | Innkeepr – Bestperformer – Seed – Premium t0–10 | facebook | conversion | 0 | 30 | - | 150000 | - | targetingOutlookDays is 30, expected 90 |
| ✅ | MissPompadour GmbH | 6a1efe081370504ba68c9581 | Innkeepr – Bestperformer – Seed – Volume t20–30 | facebook | conversion | 1 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | More | 696769a1a5250a665d0ac727 | Innkeepr - Bars - Growth - Seed - t10-20p | facebook | conversion | 4 | 90 | 0.22 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69bacbf012f9f4e3bf577932 | Innkeepr - Bars - Growth - Seed - t10-20p | facebook | conversion | 0 | 90 | 0.22 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69676960a5250a665d0aaf9b | Innkeepr - Bars - Premium - Seed - t0-10p | facebook | conversion | 4 | 30 | 0.1 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69bacc0112f9f4e3bf577d03 | Innkeepr - Bars - Premium - Seed - t0-10p | facebook | conversion | 0 | 30 | 0.1 | 150000 | - | Configuration matches defaults |
| ✅ | More | 696769b0a5250a665d0acb95 | Innkeepr - Bars - Volume - Seed - t20-30p | facebook | conversion | 4 | 90 | 0.33 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69bacbe112f9f4e3bf5775d4 | Innkeepr - Bars - Volume - Seed - t20-30p | facebook | conversion | 0 | 90 | 0.33 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69bacb5712f9f4e3bf574fcc | Innkeepr - Chunky - Growth - Seed - t10-20p | facebook | conversion | 1 | 90 | 0.22 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69bacb3312f9f4e3bf574642 | Innkeepr - Chunky - Premium - Seed - t0-10p | facebook | conversion | 1 | 30 | 0.1 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69bacb6a12f9f4e3bf575573 | Innkeepr - Chunky - Volume - Seed - t20-30p | facebook | conversion | 1 | 90 | 0.33 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69bacedb12f9f4e3bf88cbbe | Innkeepr - Iced Coffee - Growth - Seed - t10-20p | facebook | conversion | 0 | 90 | 0.22 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69299a11459c17df5fb18137 | Innkeepr - Iced Coffee - Growth - Seed - t10-20p | facebook | conversion | 4 | 90 | 0.22 | 150000 | - | Configuration matches defaults |
| ✅ | More | 68e24526120f694f532fc29c | Innkeepr - Iced Coffee - Premium - Seed - t0-10p | facebook | conversion | 4 | 30 | 0.11 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69bacec412f9f4e3bf88c61f | Innkeepr - Iced Coffee - Premium - Seed - t0-10p | facebook | conversion | 0 | 30 | 0.11 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69baceea12f9f4e3bf88d143 | Innkeepr - Iced Coffee - Volume - Seed - t20-30p | facebook | conversion | 0 | 90 | 0.33 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69299a42459c17df5fb4eeac | Innkeepr - Iced Coffee - Volume - Seed - t20-30p | facebook | conversion | 4 | 90 | 0.33 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69299b23459c17df5fc459bf | Innkeepr - Matcha - Growth - Seed - t10-20p | facebook | conversion | 4 | 90 | 0.22 | 150000 | - | Configuration matches defaults |
| ✅ | More | 68e24566120f694f53318325 | Innkeepr - Matcha - Premium - Seed - t0-10p | facebook | conversion | 4 | 30 | 0.11 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69299b35459c17df5fc460d5 | Innkeepr - Matcha - Volume - Seed - t20-30p | facebook | conversion | 4 | 90 | 0.33 | 150000 | - | Configuration matches defaults |
| ❌ | More | 69de663b0908b35406f68c89 | Innkeepr - Proteinsahne - NL - Growth - Seed - t10-20p | facebook | conversion | 0 | 30 | 0.22 | 150000 | - | targetingOutlookDays is 30, expected 90 |
| ✅ | More | 69de662d2751dd75ae38bbef | Innkeepr - Proteinsahne - NL - Premium - Seed - t0-10p | facebook | conversion | 0 | 30 | 0.1 | 150000 | - | Configuration matches defaults |
| ❌ | More | 69de66530908b35406f68c8c | Innkeepr - Proteinsahne - NL - Volume - Seed - t20-30p | facebook | conversion | 0 | 30 | 0.33 | 150000 | - | targetingOutlookDays is 30, expected 90 |
| ✅ | More | 69676be0a5250a665d55453f | Innkeepr - VMS - CH - Growth - Seed - t10-20p | facebook | conversion | 0 | 90 | 0.22 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69676bf7a5250a665d554d7f | Innkeepr - VMS - CH - Premium - Seed - t0-10p | facebook | conversion | 0 | 30 | 0.1 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69676bcda5250a665d553e46 | Innkeepr - VMS - CH - Volume - Seed - t20-30p | facebook | conversion | 0 | 90 | 0.33 | 150000 | - | Configuration matches defaults |
| ❌ | More | 69676c12a5250a665d5555f5 | Innkeepr - VMS - Growth - Seed - t10-20p | facebook | conversion | 9 | 30 | 0.21 | 150000 | - | targetingOutlookDays is 30, expected 90 |
| ✅ | More | 69676c06a5250a665d55528e | Innkeepr - VMS - Premium - Seed - t0-10p | facebook | conversion | 9 | 30 | 0.1 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69676c25a5250a665d555ceb | Innkeepr - VMS - Volume - Seed - t20-30p | facebook | conversion | 9 | 90 | 0.33 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69299c53459c17df5fe7e2f9 | Innkeepr - Zerup - Growth - Seed - t10-20p | facebook | conversion | 4 | 90 | 0.22 | 150000 | - | Configuration matches defaults |
| ✅ | More | 68e246b9120f694f533b9fc1 | Innkeepr - Zerup - Premium - Seed - t0-10p | facebook | conversion | 4 | 30 | 0.1 | 150000 | - | Configuration matches defaults |
| ✅ | More | 69299c6a459c17df5fe92393 | Innkeepr - Zerup - Volume - Seed - t20-30p | facebook | conversion | 4 | 90 | 0.33 | 150000 | - | Configuration matches defaults |
| ✅ | Nikin | 6847ea3657bdaeb51ed9e87d | Innkeepr - General - Seed | facebook | conversion | 0 | 90 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Plantura | 69a0825b2568bd24bcf3d2a6 | Innkeepr - Outdoor Pflanzen - Growth - Seed - t10-20p | facebook | conversion | 0 | 90 | 0.25 | 150000 | - | Configuration matches defaults |
| ✅ | Plantura | 69a082382568bd24bcf3c9f4 | Innkeepr - Outdoor Pflanzen - Premium - Seed - t0-10p | facebook | conversion | 0 | 30 | 0.15 | 150000 | - | Configuration matches defaults |
| ✅ | Plantura | 69a082742568bd24bcf3daac | Innkeepr - Outdoor Pflanzen - Volume - Seed - t20-30p | facebook | conversion | 0 | 90 | 0.36 | 150000 | - | Configuration matches defaults |
| ✅ | Plantura | 69a082982568bd24bcf3e4ee | Innkeepr - Retterboxen - Growth - Seed - t10-20p | facebook | conversion | 0 | 90 | 0.25 | 150000 | - | Configuration matches defaults |
| ✅ | Plantura | 69a082862568bd24bcf3de51 | Innkeepr - Retterboxen - Premium - Seed - t0-10p | facebook | conversion | 0 | 30 | 0.15 | 150000 | - | Configuration matches defaults |
| ✅ | Plantura | 69a082a72568bd24bcf3e8a7 | Innkeepr - Retterboxen - Volume - Seed - t20-30p | facebook | conversion | 0 | 90 | 0.35 | 150000 | - | Configuration matches defaults |
| ✅ | Rosental | 67f61f9f0b17ce11bdc8960e | Innkeepr - BLEM - High Impact - Seed | facebook | causal | 18 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | Rosental | 6980c4baf7564868fec23572 | Innkeepr - Beauty Doc - Seed | facebook | conversion | 0 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | Rosental | 68d4f85a0dd10e3a312f5e4d | Innkeepr - Beauty Dock - High Impact - Seed | facebook | conversion | 0 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | Rosental | 69b3bbe6deec4e2c906b3fdf | Innkeepr - Botanical - Growth - Seed - t10-20p | facebook | conversion | 1 | 90 | 0.25 | 150000 | - | Configuration matches defaults |
| ✅ | Rosental | 69b3bbbedeec4e2c906b3679 | Innkeepr - Botanical - Premium - Seed - t0-10p | facebook | conversion | 0 | 30 | 0.14 | 150000 | - | Configuration matches defaults |
| ✅ | Rosental | 69b3bbf4deec4e2c906b42f4 | Innkeepr - Botanical - Volume - Seed - t20-30p | facebook | conversion | 0 | 90 | 0.36 | 150000 | - | Configuration matches defaults |
| ❌ | Rosental | 6980c47ff7564868feb8123d | Innkeepr - EMS - Seed | facebook | conversion | 0 | 30 | - | 150000 | - | targetingOutlookDays is 30, expected 90 |
| ✅ | Rosental | 678a5ca0f3abb5123264a902 | Innkeepr - LED Mask - Seed | facebook | causal | 40 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | Rosental | 68e926d4b236f67910699632 | Innkeepr - NL - BB Reihe - Bestseller - High Impact - Seed | facebook | conversion | 23 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | Rosental | 69245716105ef186b0aec061 | Innkeepr - NL - BB Reihe - Bestseller - Seed - t10-20p | facebook | conversion | 17 | 90 | 0.23 | 150000 | - | Configuration matches defaults |
| ✅ | Rosental | 69245784105ef186b0b1c70f | Innkeepr - NL - BB Reihe - Bestseller - Seed - t20-30p | facebook | conversion | 17 | 90 | 0.35 | 150000 | - | Configuration matches defaults |
| ✅ | Rosental | 68e92627b236f67910678fbd | Innkeepr - NL - Botanical - Bestseller - High Impact -Seed | facebook | conversion | 17 | 90 | 0.07 | 150000 | - | Configuration matches defaults |
| ✅ | Rosental | 69245927105ef186b0c028fb | Innkeepr - NL - Botanical - Bestseller - Seed - t15-30p | facebook | conversion | 16 | 90 | 0.3 | - | - | Configuration matches defaults |
| ✅ | Rosental | 69245850105ef186b0b853ae | Innkeepr - NL - Botanical - Bestseller - Seed - t7-15p | facebook | conversion | 16 | 90 | 0.15 | - | - | Configuration matches defaults |
| ✅ | Rosental | 68e92747b236f679106a375c | Innkeepr - NL - Sonstiges - Bestseller - High Impact - Seed | facebook | causal | 35 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | Rosental | 69245b61105ef186b0d680dd | Innkeepr - NL - Sonstiges - Bestseller - Seed - t10-20p | facebook | causal | 34 | 90 | 0.3 | - | - | Configuration matches defaults |
| ✅ | Rosental | 69245bba105ef186b0db413c | Innkeepr - NL - Sonstiges - Bestseller - Seed - t20-30p | facebook | causal | 35 | 90 | 0.3 | - | - | Configuration matches defaults |
| ✅ | Rosental | 6942b89ba5250a665d35d601 | Innkeepr - Slow Aging - Growth - Seed - t10-20p | facebook | conversion | 0 | 90 | 0.25 | 150000 | - | Configuration matches defaults |
| ✅ | Rosental | 68d4f82b0dd10e3a312f3bc3 | Innkeepr - Slow Aging - High Impact - Seed | facebook | conversion | 0 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | Rosental | 6942b951a5250a665d360549 | Innkeepr - Slow Aging - Volume - Seed - t20-30p | facebook | conversion | 0 | 90 | 0.36 | 150000 | - | Configuration matches defaults |
| ✅ | Störtebekker | 69ef861d50b5382f00c18e3d | Innkeepr - Duschset - Growth - Seed - t10-20p | facebook | conversion | 1 | 90 | 0.22 | 150000 | - | Configuration matches defaults |
| ✅ | Störtebekker | 69ef862d50b5382f00c1924d | Innkeepr - Duschset - Premium - Seed - t0-10p | facebook | conversion | 3 | 30 | 0.11 | 150000 | - | Configuration matches defaults |
| ✅ | Störtebekker | 69ef861050b5382f00c18a78 | Innkeepr - Duschset - Volume - Seed - t20-30p | facebook | conversion | 1 | 90 | 0.34 | 150000 | - | Configuration matches defaults |
| ✅ | Störtebekker | 69ef85e250b5382f00c17d88 | Innkeepr - Probierset - Growth - Seed - t10-20p | facebook | conversion | 0 | 90 | 0.23 | 150000 | - | Configuration matches defaults |
| ✅ | Störtebekker | 69ef85d050b5382f00c178d3 | Innkeepr - Probierset - Premium - Seed - t0-10p | facebook | conversion | 0 | 30 | 0.11 | 150000 | - | Configuration matches defaults |
| ✅ | Störtebekker | 69ef85f050b5382f00c18130 | Innkeepr - Probierset - Volume - Seed - t20-30p | facebook | conversion | 0 | 90 | 0.34 | 150000 | - | Configuration matches defaults |
| ❌ | Vioneers | 63d7b7692393f1ea76dd8531 | Innkeepr - General - Seed | facebook | conversion | 10 | 720 | 0.6 | - | - | targetingOutlookDays is 720, expected 90; small customer setup |
| ✅ | ahead-nutrition.com | 6a2288d11370504ba6f636b8 | Innkeepr - Schokoriegel - Premium - Seed - t0-10p | facebook | conversion | 0 | 30 | - | 150000 | - | Configuration matches defaults |
| ✅ | ahead-nutrition.com | 6a2287e81370504ba6f61bf0 | Innkeepr - Schokoriegel - Volume - Seed - t20-30p | facebook | conversion | 0 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | ahead-nutrition.com | 6a2288c31370504ba6f634d9 | Innkeepr - Schokoriegel CH - Premium - Seed - t0-10p | facebook | conversion | 0 | 30 | - | 150000 | - | Configuration matches defaults |
| ✅ | ahead-nutrition.com | 6a2287f61370504ba6f61d6a | Innkeepr - Schokoriegel CH - Volume - Seed - t20-30p | facebook | conversion | 0 | 90 | 0.34 | 150000 | - | Configuration matches defaults |
| ✅ | ahead-nutrition.com | 6a2287991370504ba6f612ca | Innkeepr - Spreads - Growth - Seed - t10-20p | facebook | conversion | 0 | 90 | 0.24 | 150000 | - | Configuration matches defaults |
| ✅ | ahead-nutrition.com | 6a22871b1370504ba6f604ba | Innkeepr - Spreads - Premium - Seed - t0-10p | facebook | conversion | 0 | 30 | - | 150000 | - | Configuration matches defaults |
| ✅ | ahead-nutrition.com | 6a2287cd1370504ba6f61866 | Innkeepr - Spreads - Volume - Seed - t20-30p | facebook | conversion | 0 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | ahead-nutrition.com | 6a2287a91370504ba6f614e4 | Innkeepr - Spreads CH - Growth - Seed - t10-20p | facebook | conversion | 0 | 90 | 0.22 | 150000 | - | Configuration matches defaults |
| ✅ | ahead-nutrition.com | 6a2287871370504ba6f610c7 | Innkeepr - Spreads CH - Premium - Seed - t0-10p | facebook | conversion | 0 | 30 | - | 150000 | - | Configuration matches defaults |
| ✅ | ahead-nutrition.com | 6a2287bd1370504ba6f61682 | Innkeepr - Spreads CH - Volume - Seed - t20-30p | facebook | conversion | 0 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | ahead-nutrition.com | 6a2288131370504ba6f620dc | nnkeepr - Schokoriegel - Growth - Seed - t10-20p | facebook | conversion | 0 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | ahead-nutrition.com | 6a2288041370504ba6f61ed2 | nnkeepr - Schokoriegel CH - Growth - Seed - t10-20p | facebook | conversion | 0 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | to teach | 684549f157bdaeb51e6a09c0 | Innkeepr - Existing Registrations - Seed | facebook | conversion | 11 | 90 | 0.91 | - | - | Configuration matches defaults |
| ✅ | to teach | 684549de57bdaeb51e6a04e2 | Innkeepr - Likely Registrations - Seed | facebook | conversion | 6 | 90 | 0.96 | - | - | Configuration matches defaults |
| ✅ | to teach | 68c82905d594535196566f11 | Innkeepr - Subscription - Seed | facebook | conversion | 1 | 90 | 0.9 | - | - | Configuration matches defaults |

### googleAnalytics

**44** audiences (32 correct, 12 incorrect).

| Status | Workspace | `audience.id` | Audience | Source | `model.type` | `audience.treatments.count` | Outlook | Size % | Size | `exclude_visitors` | Result |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ❌ | Autobatterienbilliger | 69ca916f241e66980f915f30 | Innkeepr - General - Seed #1 | googleAnalytics | conversion | 7 | 90 | 0.5 | 150000 | - | targetingOutlookDays is 90, expected 30 |
| ✅ | Autobatterienbilliger | 69ca9146241e66980f915198 | Innkeepr - General - Seed #2 | googleAnalytics | conversion | 7 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ❌ | Clockin | 695e3a03a5250a665de6a3dd | Innkeepr - ZeiterfassungComputer - Seed #2 | googleAnalytics | conversion | 15 | 180 | 0.5 | 150000 | - | targetingOutlookDays is 180, expected 30 |
| ❌ | Clockin | 695e3aeaa5250a665deb1a15 | Innkeepr - ZeiterfassungComputer - Seed #3 | googleAnalytics | conversion | 15 | 90 | 0.5 | 150000 | - | targetingOutlookDays is 90, expected 30 |
| ❌ | Ective | 654199d5c55281d53441fdf2 | Innkeepr - General - Seed | googleAnalytics | conversion | 179 | 180 | 0.8 | - | - | targetingOutlookDays is 180, expected 30; audienceSizePercentage is 0.8, expected 0.5 |
| ✅ | Ective | 69eb91ce62dfd69717cb9a0c | Innkeepr - General - Seed #2 | googleAnalytics | conversion | 118 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ❌ | Innkeepr | 6a2749781370504ba691c3d1 | Innkeepr Google Seed | googleAnalytics | - | 1 | - | - | - | - | targetingOutlookDays is None, expected 30; audienceSizePercentage is None, expected to be defined (0.5) |
| ✅ | Junglueck | 6900d051857b19e246f05f60 | Innkeepr - Bundles & Sets - Seed #2 | googleAnalytics | conversion | 0 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Junglueck | 66d179bfc98dbc05719f7799 | Innkeepr - Feuchtigkeitspflege - Seed #1 | googleAnalytics | causal | 6 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Junglueck | 66d179d3c98dbc05719f7a2a | Innkeepr - Feuchtigkeitspflege - Seed #2 | googleAnalytics | causal | 6 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Junglueck | 6900d0c3857b19e246f147c3 | Innkeepr - Seren & Öle - Seed #1 | googleAnalytics | conversion | 15 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Junglueck | 6900d136857b19e246f22452 | Innkeepr - Seren & Öle - Seed #2 | googleAnalytics | conversion | 15 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Junglueck | 66d17985c98dbc05719f6ebe | Innkeepr - Seren - Seed #1 | googleAnalytics | causal | 22 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Junglueck | 66d1799ac98dbc05719f7252 | Innkeepr - Seren - Seed #2 | googleAnalytics | causal | 22 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Kfzteile24 | 6835d72d13526dc3d1229132 | Innkeepr - Catch All - Seed #1 | googleAnalytics | conversion | 4 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Kfzteile24 | 6835d77813526dc3d1229978 | Innkeepr - Catch All - Seed #2 | googleAnalytics | conversion | 4 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Kfzteile24 | 6835d78e13526dc3d1229c1e | Innkeepr - OE KVI - Seed #1 | googleAnalytics | conversion | 2 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Kfzteile24 | 6835d79f13526dc3d1229de1 | Innkeepr - OE KVI - Seed #2 | googleAnalytics | conversion | 2 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ❌ | LILLYDOO | 68d10cd02a849e5721606ff4 | Innkeepr - PMax ES - Seed #1 | googleAnalytics | conversion | 9 | 90 | 0.5 | 150000 | - | targetingOutlookDays is 90, expected 30 |
| ❌ | LILLYDOO | 68d10ce22a849e5721607b84 | Innkeepr - PMax ES - Seed #2 | googleAnalytics | conversion | 9 | 90 | 0.5 | 150000 | - | targetingOutlookDays is 90, expected 30 |
| ✅ | LILLYDOO | 68d10b172a849e57215f5cf8 | Innkeepr - PMax IT - Seed #1 | googleAnalytics | conversion | 8 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | LILLYDOO | 68d10cab2a849e572160592c | Innkeepr - PMax IT - Seed #2 | googleAnalytics | conversion | 8 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | MissPompadour GmbH | 6a1fca521370504ba6a5679f | Innkeepr – PMAX_Lacke – Seeds #1 | googleAnalytics | conversion | 0 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | MissPompadour GmbH | 6a1fca661370504ba6a56943 | Innkeepr – PMAX_Lacke – Seeds #2 | googleAnalytics | conversion | 0 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | MissPompadour GmbH | 6a1fca801370504ba6a56c6e | Innkeepr – PMAX_Zubehör – Seeds #1 | googleAnalytics | conversion | 0 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | MissPompadour GmbH | 6a1fca961370504ba6a56eef | Innkeepr – PMAX_Zubehör – Seeds #2 | googleAnalytics | conversion | 0 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ❌ | Pendix | 65dde29ef4f24f03238ac989 | Innkeepr - Pmax - Seed | googleAnalytics | causal | 6 | 60 | 0.6 | - | - | targetingOutlookDays is 60, expected 30; audienceSizePercentage is 0.6, expected 0.5 |
| ✅ | Plantura | 69956f91bf21699e6ec8fcff | Innkeepr - DE_2024_PMAX_Mid-Lowseller - Seed #1 | googleAnalytics | conversion | 1 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Plantura | 69956facbf21699e6ec90585 | Innkeepr - DE_2024_PMAX_Mid-Lowseller - Seed #2 | googleAnalytics | conversion | 1 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ❌ | Plantura | 69956feebf21699e6ec9181b | Innkeepr - DE_2024_PMAX_Motten - Seed #1 | googleAnalytics | conversion | 3 | 60 | 0.5 | 150000 | - | targetingOutlookDays is 60, expected 30 |
| ✅ | Plantura | 69957000bf21699e6ec91ea6 | Innkeepr - DE_2024_PMAX_Motten - Seed #2 | googleAnalytics | conversion | 3 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Plantura | 699570c8bf21699e6eda4a8f | Innkeepr - DE_2024_PMAX_Outdoor - Seed #1 | googleAnalytics | conversion | 8 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Plantura | 69957073bf21699e6eda3055 | Innkeepr - DE_2024_PMAX_Outdoor - Seed #2 | googleAnalytics | conversion | 8 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Plantura | 69936b8ebf21699e6e310a13 | Innkeepr - DE_2024_PMAX_Trauermücken - Seed #1 | googleAnalytics | causal | 13 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Plantura | 69956f03bf21699e6ec8d30c | Innkeepr - DE_2024_PMAX_Trauermücken - Seed #2 | googleAnalytics | causal | 13 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Störtebekker | 69fc8ede886052eb059b7d4e | Innkeepr - PMax - Seed #1 | googleAnalytics | conversion | 0 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Störtebekker | 69fc8eef886052eb05c096b1 | Innkeepr - PMax - Seed #2 | googleAnalytics | conversion | 0 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Tchibo | 69f71bfc1ef3b5814539f69c | Innkeepr - Kids - Seed #1 | googleAnalytics | conversion | 0 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Tchibo | 69f71bdf1ef3b5814539e17d | Innkeepr - Kids - Seed #2 | googleAnalytics | conversion | 0 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Tchibo | 69f71b921ef3b5814527d000 | Innkeepr - Ladies - Seed #1 | googleAnalytics | conversion | 2 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ✅ | Tchibo | 69f71bb61ef3b5814539c0ed | Innkeepr - Ladies - Seed #2 | googleAnalytics | conversion | 1 | 30 | 0.5 | 150000 | - | Configuration matches defaults |
| ❌ | Vioneers | 6392fae63ceef43bf3448b98 | Innkeepr - General - Seed | googleAnalytics | conversion | 11 | 720 | 0.96 | - | - | targetingOutlookDays is 720, expected 30; audienceSizePercentage is 0.96, expected 0.5; small customer setup |
| ❌ | to teach | 69305bdcb75fb863fe6aa763 | Innkeepr - PMax - Seed #1 | googleAnalytics | conversion | 3 | 90 | 0.5 | 150000 | - | targetingOutlookDays is 90, expected 30 |
| ❌ | to teach | 69305c06b75fb863fe6aa764 | Innkeepr - PMax - Seed #2 | googleAnalytics | conversion | 3 | 90 | 0.5 | 150000 | - | targetingOutlookDays is 90, expected 30 |

### tiktok

**1** audiences (0 correct, 1 incorrect).

| Status | Workspace | `audience.id` | Audience | Source | `model.type` | `audience.treatments.count` | Outlook | Size % | Size | `exclude_visitors` | Result |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ❌ | LILLYDOO | 67d1b8102eb78f8bcc5e2c57 | Innkeepr - General - Seed | tiktok | conversion | 0 | 60 | 0.5 | - | - | targetingOutlookDays is 60, expected 30 |

## value-based

**17** active audiences (17 correct, 0 incorrect).

### facebook

**17** audiences (17 correct, 0 incorrect).

| Status | Workspace | `audience.id` | Audience | Source | `model.type` | `audience.treatments.count` | Outlook | Size % | Size | `exclude_visitors` | Result |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ✅ | LILLYDOO | 6836db0013526dc3d135eb7d | Innkeepr - Seed - ValueBased | facebook | conversion | 14 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | LILLYDOO | 68cbfcaa77b55cfe74111c89 | Innkeepr - Seed CHAT - ValueBased | facebook | conversion | 7 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | LILLYDOO | 68cacdfe77b55cfe7478372b | Innkeepr - Seed ES - ValueBased | facebook | conversion | 3 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | LILLYDOO | 6836dbd913526dc3d1360591 | Innkeepr - Seed IT - ValueBased | facebook | conversion | 0 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | Nikin | 6847ea9d57bdaeb51ed9f3e9 | Innkeepr - Seed - ValueBased | facebook | conversion | 0 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | Rosental | 6980c50ef7564868fec25101 | Innkeepr - Beauty Doc - ValueBased | facebook | conversion | 0 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | Rosental | 68518c34a93b88d67a15d001 | Innkeepr - Botanical - ValueBased | facebook | causal | 57 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | Rosental | 6980c4eaf7564868fec2469d | Innkeepr - EMS - ValueBased | facebook | conversion | 0 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | Rosental | 68518c65a93b88d67a15d581 | Innkeepr - LED - ValueBased | facebook | causal | 30 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | Störtebekker | 69f34a7151f70b57e4315623 | Innkeepr - Duschset - ValueBased | facebook | conversion | 1 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | Störtebekker | 69f34a5f51f70b57e43153b5 | Innkeepr - Probierset - ValueBased | facebook | conversion | 0 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | ahead-nutrition.com | 6a22b4211370504ba6fad854 | Innkeepr - Schokoriegel - ValueBased | facebook | causal | 7 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | ahead-nutrition.com | 6a22b4461370504ba6fadb86 | Innkeepr - Schokoriegel - ValueBased- CH | facebook | conversion | 7 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | ahead-nutrition.com | 6a27cbcf1370504ba69d7297 | Innkeepr - Spreads - ValueBased | facebook | conversion | 5 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | ahead-nutrition.com | 6a27cbf91370504ba69d77b3 | Innkeepr - Spreads - ValueBased - CH | facebook | conversion | 5 | 90 | - | 150000 | - | Configuration matches defaults |
| ✅ | to teach | 68fb486c857b19e246f15fdd | Innkeepr - Registrations - Value-based - Seed | facebook | conversion | 26 | 90 | 1 | - | - | Configuration matches defaults |
| ✅ | to teach | 68fb4747857b19e246ef7617 | Innkeepr - Subscriptions - Value-based - Seed | facebook | conversion | 16 | 90 | 1 | - | - | Configuration matches defaults |
