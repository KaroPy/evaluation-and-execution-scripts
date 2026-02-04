# Google Ads Conversion Signals Analysis

**Date:** 2025-01-13

## Context

We use brand and generic campaigns in a customer Google Ads account. The traffic is evaluated by brand and generic traffic. The most valuable traffic is then sent to a custom goal in the Google Ads conversion setting (Upgraded Plan Signals - CB). The control group (Upgraded Plan Signals) just uses thresholds which are applied to the conversion probability.

### Campaign Setup

| Campaign | Type | Conversion Goal |
|----------|------|-----------------|
| Campaign 1 | Brand | Upgraded Plan Signals (Control) |
| Campaign 1a | Brand | Upgraded Plan Signals - CB |
| Campaign 2 | Generic | Upgraded Plan Signals - CB |

### Key Questions

1. Why does the control setting work better?
2. Why is Campaign 2 (Generic) spend not increasing and very low?

---

## Analysis: Why Control (Upgraded Plan Signals) Might Work Better

1. **Signal Volume & Learning**: The threshold-based conversion goal likely has more conversion volume, giving Google's Smart Bidding algorithm more data to optimize. The "-CB" custom goal may have fewer conversions, making it harder for the algorithm to learn patterns.

2. **Conversion Lag**: If the custom "-CB" goal has longer attribution windows or delayed conversions, Smart Bidding struggles to connect ad interactions to outcomes quickly.

3. **Signal Quality vs Quantity Trade-off**: While the custom "-CB" goal sends "more valuable" traffic signals, if the volume is too low, the algorithm can't effectively optimize. Google's algorithms need ~30-50 conversions per month minimum to work well.

---

## Analysis: Why Campaign 2 (Generic) Has Low Spend

Several likely causes:

1. **Limited Conversion Data**: Generic campaigns typically have lower conversion rates than brand. Combined with the restrictive "-CB" goal, there may be very few conversions for the algorithm to learn from → it doesn't know when/how to bid → spend stays low.

2. **Bid Strategy Constraints**: If using Target CPA or Target ROAS, and the algorithm can't find traffic that meets targets with the "-CB" conversion goal, it simply won't spend.

3. **Audience Pool Size**: The "-CB" goal filters for "most valuable traffic" - in generic campaigns, this pool might be very small, severely limiting eligible impressions.

4. **Cold Start Problem**: New conversion actions need time to accumulate data. The algorithm may be stuck in a conservative mode.

---

## Conversion Volume Data

**Finding:** For all three settings, the same amount is uploaded (27 conversions for the last 30 days).

This is quite low for Smart Bidding optimization (ideally 30-50+ per month minimum).

### The Core Issue: Conversion Distribution

Even though 27 conversions are uploaded to each goal, the conversions are likely attributed differently across campaigns:

| Campaign | Conversion Goal | Likely Conversion Attribution |
|----------|----------------|------------------------------|
| Campaign 1 (Brand) | Upgraded Plan Signals | Gets most conversions (brand traffic converts well) |
| Campaign 1a (Brand) | Upgraded Plan Signals - CB | Gets some conversions |
| Campaign 2 (Generic) | Upgraded Plan Signals - CB | Gets very few conversions |

**Generic campaigns typically have much lower conversion rates than brand campaigns.** If the 27 conversions uploaded to "Upgraded Plan Signals - CB" are mostly attributed to brand clicks (Campaign 1a), then Campaign 2 (Generic) sees almost no conversions → Smart Bidding has no signal to optimize → it doesn't spend.

### Validation Questions

1. In Google Ads, how many conversions does each campaign show individually for the last 30 days?
2. What's the conversion rate per campaign?
3. Is Campaign 2 showing "Learning Limited" status?

---

## Proposed Solution: Hybrid/Composite Conversion Goal

### Concept

Create a custom goal in Google Ads that combines both optimized conversion uploads AND standard conversions to improve campaign ROAS.

### Why This Could Help

#### 1. More Signal Volume for Learning
- Optimized "-CB" conversions provide **quality signals** (most valuable traffic)
- Standard conversions add **quantity signals** (more data points)
- Together: Smart Bidding gets enough volume to learn patterns while still being guided toward valuable traffic

#### 2. Weighted Optimization

Assign different conversion values to each action:

| Conversion Action | Value Weight | Purpose |
|-------------------|--------------|---------|
| Optimized CB Upload | Higher (e.g., €10) | Steers algorithm toward valuable traffic |
| Standard Conversion | Lower (e.g., €1-3) | Provides learning volume |

This way, the algorithm optimizes for total value, prioritizing high-value signals while still having enough data to learn bidding patterns.

#### 3. Solves the Generic Campaign Problem

Campaign 2 (Generic) would finally get conversion signals to learn from, allowing it to spend and find valuable generic traffic.

### Implementation Steps

1. **Create a Custom Goal** (Goals → Conversions → Custom goals)
2. **Add both conversion actions** to the goal
3. **Set appropriate values** for each action (reflecting relative importance)
4. **Apply the custom goal** as the campaign-level conversion goal

### Considerations / Risks

- **Avoid double-counting**: Make sure the same user journey isn't counted in both conversion actions
- **Value calibration**: Getting the relative weights right may require testing
- **Monitor closely**: Compare ROAS before/after over 2-4 weeks

---

## Next Steps

1. Check per-campaign conversion breakdown in Google Ads
2. Verify if Campaign 2 shows "Learning Limited" status
3. Determine value weighting strategy for hybrid goal
4. Analyze overlap between optimized and standard conversions